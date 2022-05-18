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
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;

public class AesEcbValueTokenizerFactory extends ValueTokenizerFactory implements Serializable {

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
