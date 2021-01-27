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

package com.google.cloud.solutions.autotokenize.pipeline.encryptors;

import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.DeterministicAead;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.daead.DeterministicAeadConfig;
import java.io.IOException;
import java.io.Serializable;
import java.security.GeneralSecurityException;

/**
 * Creates a DAEAD encrypting tokenizer for the Cleartext Key derived by unwrapping the key using
 * Google Cloud KMS.
 */
public class DaeadEncryptingValueTokenizerFactory implements ValueTokenizerFactory, Serializable {

  private final String cleartextKeysetJson;

  /**
   * Instantiates the ValueTokenizerFactory with the clearText Data encryption key.
   *
   * @param cleartextKeysetJson the clear-text data encryption key derived by unwrapping the
   * provided key using GCP KMS.
   */
  public DaeadEncryptingValueTokenizerFactory(String cleartextKeysetJson) {
    this.cleartextKeysetJson = cleartextKeysetJson;
  }

  @Override
  public DaeadEncryptingValueTokenizer makeValueTokenizer() {
    try {
      DeterministicAeadConfig.register();
      return new DaeadEncryptingValueTokenizer(buildCrypto());
    } catch (IOException | GeneralSecurityException exp) {
      throw new TinkInitException(exp);
    }
  }

  private DeterministicAead buildCrypto() throws IOException, GeneralSecurityException {
    return
        CleartextKeysetHandle
            .read(JsonKeysetReader.withString(cleartextKeysetJson))
            .getPrimitive(DeterministicAead.class);
  }

  public static class TinkInitException extends RuntimeException {

    public TinkInitException(Throwable cause) {
      super("Error initializing TinkCrypto", cause);
    }
  }
}
