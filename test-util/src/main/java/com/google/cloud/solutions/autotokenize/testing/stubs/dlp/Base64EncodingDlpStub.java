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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.dlp.v2.stub.DlpServiceStub;
import com.google.cloud.solutions.autotokenize.testing.stubs.BaseUnaryApiFuture;
import com.google.cloud.solutions.autotokenize.testing.stubs.TestingBackgroundResource;
import com.google.common.collect.ImmutableList;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Table.Row;
import com.google.privacy.dlp.v2.Value;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Base64EncodingDlpStub extends DlpServiceStub implements Serializable {

  private final String recordIdColumnName;
  private final ImmutableList<FieldId> expectedHeaders;
  private final String projectId;

  public Base64EncodingDlpStub(
      String recordIdColumnName, ImmutableList<String> expectedHeaders, String projectId) {
    this.recordIdColumnName = recordIdColumnName;
    this.expectedHeaders =
        expectedHeaders.stream()
            .map(h -> FieldId.newBuilder().setName(h).build())
            .collect(toImmutableList());
    this.projectId = projectId;
  }

  @Override
  public UnaryCallable<DeidentifyContentRequest, DeidentifyContentResponse>
      deidentifyContentCallable() {
    return new UnaryCallable<DeidentifyContentRequest, DeidentifyContentResponse>() {
      @Override
      public ApiFuture<DeidentifyContentResponse> futureCall(
          DeidentifyContentRequest deidentifyContentRequest, ApiCallContext apiCallContext) {
        return new BaseUnaryApiFuture<DeidentifyContentResponse>() {
          @Override
          public DeidentifyContentResponse get() {

            assertThat(deidentifyContentRequest.getParent())
                .isEqualTo(String.format("projects/%s", projectId));

            List<FieldTransformation> fieldTransformations =
                deidentifyContentRequest
                    .getDeidentifyConfig()
                    .getRecordTransformations()
                    .getFieldTransformationsList();

            List<FieldId> headers = deidentifyContentRequest.getItem().getTable().getHeadersList();

            assertThat(headers)
                .containsAnyIn(
                    ImmutableList.builder()
                        .add(recordIdColumnName)
                        .addAll(expectedHeaders)
                        .build());

            int recordIdColumnIndex =
                deidentifyContentRequest
                    .getItem()
                    .getTable()
                    .getHeadersList()
                    .indexOf(FieldId.newBuilder().setName(recordIdColumnName).build());

            List<Row> updatedRows =
                deidentifyContentRequest.getItem().getTable().getRowsList().stream()
                    .map(
                        row -> {
                          List<Value> valuesList = row.getValuesList();
                          ImmutableList.Builder<Value> outputValueBuilder = ImmutableList.builder();

                          for (int i = 0; i < valuesList.size(); i++) {

                            Value value = valuesList.get(i);
                            outputValueBuilder.add(
                                (i == recordIdColumnIndex) ? value : encodeBase64Value(value));
                          }

                          return row.toBuilder()
                              .clearValues()
                              .addAllValues(outputValueBuilder.build())
                              .build();
                        })
                    .collect(toImmutableList());

            return DeidentifyContentResponse.newBuilder()
                .setItem(
                    ContentItem.newBuilder()
                        .setTable(Table.newBuilder().addAllHeaders(headers).addAllRows(updatedRows))
                        .build())
                .build();
          }
        };
      }
    };
  }

  private static Value encodeBase64Value(Value value) {

    byte[] bytes = null;

    switch (value.getTypeCase()) {
      case INTEGER_VALUE:
        bytes = ByteBuffer.allocate(Long.BYTES).putLong(value.getIntegerValue()).array();
        break;
      case FLOAT_VALUE:
        bytes = ByteBuffer.allocate(Double.BYTES).putDouble(value.getIntegerValue()).array();
        break;
      case STRING_VALUE:
        bytes = value.getStringValue().getBytes();
        break;
      case BOOLEAN_VALUE:
        bytes = ByteBuffer.allocate(Integer.BYTES).putInt(value.getBooleanValue() ? 1 : 0).array();
        break;
      case TIMESTAMP_VALUE:
      case TIME_VALUE:
      case DATE_VALUE:
      case DAY_OF_WEEK_VALUE:
      case TYPE_NOT_SET:
        return Value.getDefaultInstance();
    }

    return Value.newBuilder().setStringValue(Base64.getEncoder().encodeToString(bytes)).build();
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
