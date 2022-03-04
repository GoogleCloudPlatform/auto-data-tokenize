/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.solutions.autotokenize.encryptors;

import static org.apache.arrow.util.Preconditions.checkArgument;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.KeyMaterialType;
import com.google.common.io.BaseEncoding;
import com.google.privacy.dlp.v2.Value;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * Encrypts the provided String value using AES128-ECB encryption.
 *
 * <p>ECB mode of encryption is not secure and should not be used in production environments. This
 * class simply demonstrates a sample custom tokenizer that can be used with the pipeline.
 */
public class AesEcbStringValueTokenizer implements ValueTokenizer {

  private final Cipher encryptCipher;
  private final Cipher decryptCipher;

  public AesEcbStringValueTokenizer(byte[] keyBytes) throws GeneralSecurityException {
    var secretKey = new SecretKeySpec(keyBytes, "AES");
    this.encryptCipher = makeCipher(Cipher.ENCRYPT_MODE, secretKey);
    this.decryptCipher = makeCipher(Cipher.DECRYPT_MODE, secretKey);
  }

  @Override
  public String encrypt(Value value) throws ValueTokenizingException {
    try {
      return BaseEncoding.base64()
          .encode(encryptCipher.doFinal(value.getStringValue().getBytes(StandardCharsets.UTF_8)));

    } catch (GeneralSecurityException securityException) {
      throw new ValueTokenizingException("error encrypting value", securityException);
    }
  }

  @Override
  public Value decrypt(String cipherText) throws ValueTokenizingException {

    try {
      var plainText =
          new String(
              decryptCipher.doFinal(BaseEncoding.base64().decode(cipherText)),
              StandardCharsets.UTF_8);

      return Value.newBuilder().setStringValue(plainText).build();
    } catch (GeneralSecurityException securityException) {
      throw new ValueTokenizingException("error decypting value", securityException);
    }
  }

  private static Cipher makeCipher(int opMode, SecretKeySpec secretKey)
      throws GeneralSecurityException {
    var cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
    cipher.init(opMode, secretKey);
    return cipher;
  }

  public static class AesEcbValueTokenizerFactory extends ValueTokenizerFactory
      implements Serializable {

    public AesEcbValueTokenizerFactory(String keyString, KeyMaterialType keyMaterialType) {
      super(keyString, keyMaterialType);
    }

    @Override
    public AesEcbStringValueTokenizer makeValueTokenizer() {

      try {

        checkArgument(
            KeyMaterialType.RAW_BASE64_KEY.equals(keyMaterialType)
                || KeyMaterialType.RAW_UTF8_KEY.equals(keyMaterialType),
            "expected keyMaterialType (RAW_BASE64_KEY or RAW_UTF8_KEY). Found %s",
            keyMaterialType);

        var keyBytes =
            (keyMaterialType.equals(KeyMaterialType.RAW_UTF8_KEY))
                ? keyString.getBytes(StandardCharsets.UTF_8)
                : BaseEncoding.base64().decode(keyString);

        return new AesEcbStringValueTokenizer(keyBytes);
      } catch (GeneralSecurityException generalSecurityException) {
        throw new RuntimeException(
            "Error initializing the AES ValueTokenizer", generalSecurityException);
      }
    }
  }
}
