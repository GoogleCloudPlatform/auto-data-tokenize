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


import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretManagerServiceSettings;
import com.google.cloud.secretmanager.v1.stub.SecretManagerServiceStub;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class SecretsClient {
  public static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  @Nullable private final SecretManagerServiceStub stub;
  private final SecretManagerServiceSettings settings;

  private SecretsClient(@Nullable SecretManagerServiceStub stub) {
    this(stub, null);
  }

  private SecretsClient(@Nullable SecretManagerServiceStub stub, @Nullable SecretManagerServiceSettings settings) {
    this.stub = stub;
    if (settings == null) {
      try {
        this.settings = SecretManagerServiceSettings.newBuilder().build();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      this.settings = settings;
    }
  }


  public static SecretsClient of() {
    return new SecretsClient(null);
  }

  public static SecretsClient withSecretsStub(SecretManagerServiceStub stub) {
    return new SecretsClient(stub);
  }

  public static SecretsClient withSettings(SecretManagerServiceSettings settings) {
    return new SecretsClient(null, settings);
  }

  public String accessSecret(String secretVersionResourceId) {
    try (var secretManger = buildSecretManagerClient()) {
      logger.atInfo().log("accessing secret version: %s", secretVersionResourceId);

      var secret = secretManger.accessSecretVersion(secretVersionResourceId);
      return secret.getPayload().getData().toString(StandardCharsets.UTF_8);
    } catch (Exception exception) {
      logger.atSevere().withCause(exception).log(
          "error accessing secrets version: %s", secretVersionResourceId);
      throw new RuntimeException(exception);
    }
  }

  private SecretManagerServiceClient buildSecretManagerClient() throws IOException {
    return (stub == null)
        ? SecretManagerServiceClient.create(settings)
        : SecretManagerServiceClient.create(stub);
  }
}
