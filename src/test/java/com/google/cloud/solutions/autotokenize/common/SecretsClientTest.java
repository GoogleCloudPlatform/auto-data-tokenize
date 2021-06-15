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

package com.google.cloud.solutions.autotokenize.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.secretmanager.v1.stub.SecretManagerServiceStub;
import com.google.cloud.solutions.autotokenize.testing.stubs.secretmanager.ConstantSecretVersionValueManagerServicesStub;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class SecretsClientTest {

  private final SecretManagerServiceStub fakeStub =
      ConstantSecretVersionValueManagerServicesStub.of("id/of/my/version", "my-secret-value");

  @Test
  public void accessPasswordSecret_valid() {
    assertThat(SecretsClient.withSecretsStub(fakeStub).accessPasswordSecret("id/of/my/version"))
        .isEqualTo("my-secret-value");
  }

  @Test
  public void accessPasswordSecret_invalidKey_throwsRuntimeException() {
    var secretsService = SecretsClient.withSecretsStub(fakeStub);
    assertThrows(
        RuntimeException.class, () -> secretsService.accessPasswordSecret("unknown/secret/id"));
  }

  @Test
  public void accessPasswordSecret_noCredentials_throwsRuntimeException() {
    var secretsService = SecretsClient.of();
    assertThrows(
        RuntimeException.class,
        () ->
            secretsService.accessPasswordSecret(
                "projects/auto-dlp-jdbc/secrets/mysql-password/versions/1"));
  }
}
