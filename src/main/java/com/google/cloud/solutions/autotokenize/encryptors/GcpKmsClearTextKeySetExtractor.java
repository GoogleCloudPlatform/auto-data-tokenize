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


import com.google.auth.oauth2.GoogleCredentials;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;

public class GcpKmsClearTextKeySetExtractor implements ClearTextKeySetExtractor {

  private final String tinkKeySetJson;
  private final String mainKeyKmsUri;
  private final GoogleCredentials credential;

  public GcpKmsClearTextKeySetExtractor(String tinkKeySetJson, String mainKeyKmsUri, GoogleCredentials credentials) {
    this.tinkKeySetJson = tinkKeySetJson;
    this.mainKeyKmsUri = mainKeyKmsUri;
    this.credential = credentials;
  }

  /**
   * Unwraps the provided tink-encryption key using Cloud KMS Key-encryption-key.
   *
   * @return the plaintext encryption key-set
   * @throws GeneralSecurityException when provided wrapped Key-set is invalid.
   * @throws IOException when there is error reading from the GCP-KMS.
   */
  @Override
  public String get() throws GeneralSecurityException, IOException {
    var tinkKeySet =
            KeysetHandle.read(
                    JsonKeysetReader.withString(tinkKeySetJson),
                    new GcpKmsClient().withCredentials(credential).getAead(mainKeyKmsUri));

    var baos = new ByteArrayOutputStream();
    CleartextKeysetHandle.write(tinkKeySet, JsonKeysetWriter.withOutputStream(baos));
    return baos.toString();
  }
}
