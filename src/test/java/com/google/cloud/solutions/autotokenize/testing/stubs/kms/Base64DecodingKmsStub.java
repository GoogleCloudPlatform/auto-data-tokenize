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

package com.google.cloud.solutions.autotokenize.testing.stubs.kms;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.kms.v1.DecryptRequest;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.stub.KeyManagementServiceStub;
import com.google.cloud.solutions.autotokenize.testing.stubs.BaseUnaryApiFuture;
import com.google.cloud.solutions.autotokenize.testing.stubs.TestingBackgroundResource;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import java.util.concurrent.TimeUnit;

public class Base64DecodingKmsStub extends KeyManagementServiceStub {

  private final String expectedKmsUri;

  public Base64DecodingKmsStub(String expectedKmsUri) {
    this.expectedKmsUri = expectedKmsUri;
  }

  @Override
  public UnaryCallable<DecryptRequest, DecryptResponse> decryptCallable() {

    if (testingBackgroundResource.isShutdown()) {
      throw new RuntimeException("KMS Client is already shutdown");
    }

    return new UnaryCallable<>() {
      @Override
      public ApiFuture<DecryptResponse> futureCall(DecryptRequest request, ApiCallContext context) {
        return new BaseUnaryApiFuture<>() {
          @Override
          public DecryptResponse get() {

            assertThat(request.getName()).isEqualTo(expectedKmsUri);

            return DecryptResponse.newBuilder()
                .setPlaintext(
                    ByteString.copyFrom(
                        BaseEncoding.base64().decode(request.getCiphertext().toStringUtf8())))
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
  public boolean awaitTermination(long duration, TimeUnit unit) {
    return testingBackgroundResource.awaitTermination(duration, unit);
  }

  @Override
  public void close() {
    testingBackgroundResource.close();
  }
}
