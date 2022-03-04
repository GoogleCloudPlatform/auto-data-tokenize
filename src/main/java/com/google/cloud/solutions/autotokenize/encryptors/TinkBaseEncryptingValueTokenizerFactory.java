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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.KeyMaterialType;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.KeysetHandle;
import java.io.IOException;
import java.security.GeneralSecurityException;

public abstract class TinkBaseEncryptingValueTokenizerFactory extends ValueTokenizerFactory {

  public TinkBaseEncryptingValueTokenizerFactory(
      String keyString, KeyMaterialType keyMaterialType) {
    super(keyString, keyMaterialType);
  }

  public KeysetHandle getKeysetHandle() throws IOException, GeneralSecurityException {
    checkArgument(
        keyMaterialType.equals(KeyMaterialType.TINK_GCP_KEYSET_JSON),
        "expected KeyMaterialType=TINK_GCP_KEYSET_JSON, found %s",
        keyMaterialType.name());

    return CleartextKeysetHandle.read(JsonKeysetReader.withString(keyString));
  }

  public static class TinkInitException extends RuntimeException {
    public TinkInitException(Throwable cause) {
      super("Error initializing TinkCrypto", cause);
    }
  }
}
