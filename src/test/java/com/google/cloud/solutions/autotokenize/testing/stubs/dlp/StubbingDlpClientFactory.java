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

package com.google.cloud.solutions.autotokenize.testing.stubs.dlp;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.dlp.v2.stub.DlpServiceStub;
import com.google.cloud.solutions.autotokenize.dlp.DlpClientFactory;

public final class StubbingDlpClientFactory implements DlpClientFactory {

  private final DlpServiceStub stub;

  public StubbingDlpClientFactory(DlpServiceStub stub) {
    this.stub = stub;
  }

  @Override
  public DlpServiceClient newClient() {
    return DlpServiceClient.create(stub);
  }
}
