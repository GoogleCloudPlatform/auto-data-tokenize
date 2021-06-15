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

package com.google.cloud.solutions.autotokenize.testing.stubs.secretmanager;


import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretPayload;
import com.google.cloud.secretmanager.v1.stub.SecretManagerServiceStub;
import com.google.cloud.solutions.autotokenize.testing.stubs.BaseUnaryApiFuture;
import com.google.cloud.solutions.autotokenize.testing.stubs.TestingBackgroundResource;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/** Cloud Secret Manager stub to provide a {@code accessSecret} based on a KeyId-Value lookup. */
public class ConstantSecretVersionValueManagerServicesStub extends SecretManagerServiceStub {

  private final ImmutableMap<String, String> preconfiguredSecrets;

  public ConstantSecretVersionValueManagerServicesStub(
      ImmutableMap<String, String> preconfiguredSecrets) {
    this.preconfiguredSecrets = preconfiguredSecrets;
  }

  public static ConstantSecretVersionValueManagerServicesStub of(String keyId, String secretValue) {
    return new ConstantSecretVersionValueManagerServicesStub(ImmutableMap.of(keyId, secretValue));
  }

  private void validateSecretVersion(String versionId) {
    if (!preconfiguredSecrets.containsKey(versionId)) {
      throw new UncheckedExecutionException(new IOException("Unknown Key"));
    }
  }

  @Override
  public UnaryCallable<AccessSecretVersionRequest, AccessSecretVersionResponse>
      accessSecretVersionCallable() {
    return new UnaryCallable<>() {
      @Override
      public ApiFuture<AccessSecretVersionResponse> futureCall(
          AccessSecretVersionRequest request, ApiCallContext context) {
        return new BaseUnaryApiFuture<>() {
          @Override
          public AccessSecretVersionResponse get() {
            validateSecretVersion(request.getName());

            GoogleLogger.forEnclosingClass()
                .atInfo()
                .log(
                    "sending secret: %s -> %s",
                    request.getName(), preconfiguredSecrets.get(request.getName()));

            return AccessSecretVersionResponse.newBuilder()
                .setName(request.getName())
                .setPayload(
                    SecretPayload.newBuilder()
                        .setData(
                            ByteString.copyFrom(
                                preconfiguredSecrets.get(request.getName()),
                                StandardCharsets.UTF_8))
                        .build())
                .build();
          }
        };
      }
    };
  }

  private final TestingBackgroundResource testingBackgroundResource =
      new TestingBackgroundResource();

  @Override
  public void shutdown() {
    testingBackgroundResource.shutdown();
  }

  @Override
  public boolean isShutdown() {
    return testingBackgroundResource.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return testingBackgroundResource.isTerminated();
  }

  @Override
  public void shutdownNow() {
    testingBackgroundResource.shutdownNow();
  }

  @Override
  public boolean awaitTermination(long l, TimeUnit timeUnit) {
    return testingBackgroundResource.awaitTermination(l, timeUnit);
  }

  @Override
  public void close() {
    testingBackgroundResource.close();
  }
}
