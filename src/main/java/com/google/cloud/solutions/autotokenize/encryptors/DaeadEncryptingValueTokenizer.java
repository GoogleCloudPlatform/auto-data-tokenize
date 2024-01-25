/*
 * Copyright 2021 Google LLC
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

import com.google.crypto.tink.DeterministicAead;
import com.google.privacy.dlp.v2.Value;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;

/**
 * Provides Encrypt and Decrypt function using Deterministic AEAD provided through Google's Tink
 * library.
 */
public class DaeadEncryptingValueTokenizer implements ValueTokenizer {

  private static final byte[] DAEAD_STAMP_BYTES = "AutoDLP".getBytes(StandardCharsets.UTF_8);
  private final DeterministicAead deterministicAead;

  public DaeadEncryptingValueTokenizer(DeterministicAead deterministicAead) {
    this.deterministicAead = deterministicAead;
  }

  @Override
  public String encrypt(Value value) throws ValueTokenizingException {
    try {
      byte[] cipherBytes =
          deterministicAead.encryptDeterministically(value.toByteArray(), DAEAD_STAMP_BYTES);

      return Base64.getEncoder().encodeToString(cipherBytes);
    } catch (GeneralSecurityException exception) {
      throw new ValueTokenizingException("Error encrypting value", exception);
    }
  }

  @Override
  public Value decrypt(String cipherText) throws ValueTokenizingException {

    try {
      byte[] cipherBytes = Base64.getDecoder().decode(cipherText);

      byte[] plainBytes =
          deterministicAead.decryptDeterministically(cipherBytes, DAEAD_STAMP_BYTES);

      return Value.parseFrom(plainBytes);
    } catch (GeneralSecurityException | InvalidProtocolBufferException exception) {
      throw new ValueTokenizingException("Error decrypting value", exception);
    }
  }
}
