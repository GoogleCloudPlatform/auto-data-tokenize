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
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.dlp.v2.stub.DlpServiceStub;
import com.google.cloud.solutions.autotokenize.common.PairIterator;
import com.google.cloud.solutions.autotokenize.testing.stubs.BaseUnaryApiFuture;
import com.google.cloud.solutions.autotokenize.testing.stubs.TestingBackgroundResource;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.privacy.dlp.v2.ContentLocation;
import com.google.privacy.dlp.v2.Finding;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InspectContentRequest;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.InspectResult;
import com.google.privacy.dlp.v2.Location;
import com.google.privacy.dlp.v2.RecordLocation;
import com.google.privacy.dlp.v2.Value;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class ItemShapeValidatingDlpStub extends DlpServiceStub implements Serializable {

  private final String projectId;
  private final ImmutableSet<ImmutablePair<String, InfoType>> schemaKeyRegexInfoTypes;

  public ItemShapeValidatingDlpStub(
      String projectId, ImmutableMap<String, String> schemaKeyInfoTypeMap) {
    this.projectId = projectId;
    this.schemaKeyRegexInfoTypes =
        schemaKeyInfoTypeMap.entrySet().stream()
            .map(
                entry -> {
                  //noinspection UnstableApiUsage
                  var schemaKeyWithArrayMatcher =
                      Splitter.on('.')
                          .splitToStream(entry.getKey())
                          .map(x -> x.replaceAll("([\\$\\[\\]\\(\\)])", "\\\\$1"))
                          .map(x -> x + "(?:\\[\\d+\\])?")
                          .collect(Collectors.joining("\\."));

                  return ImmutablePair.of(
                      schemaKeyWithArrayMatcher,
                      InfoType.newBuilder().setName(entry.getValue()).build());
                })
            .collect(toImmutableSet());
  }

  @Override
  public UnaryCallable<InspectContentRequest, InspectContentResponse> inspectContentCallable() {

    return new UnaryCallable<>() {
      @Override
      public ApiFuture<InspectContentResponse> futureCall(
          InspectContentRequest inspectContentRequest, ApiCallContext context) {
        return new BaseUnaryApiFuture<>() {
          @Override
          public InspectContentResponse get() throws InterruptedException, ExecutionException {

            assertProjectIdMatches(inspectContentRequest.getParent());

            var requestTable = inspectContentRequest.getItem().getTable();

            var headersCount = requestTable.getHeadersCount();

            var headers = requestTable.getHeadersList();

            var findings =
                requestTable.getRowsList().stream()
                    .flatMap(
                        row -> {
                          assertThat(row.getValuesCount()).isEqualTo(headersCount);
                          return PairIterator.of(headers, row.getValuesList()).stream();
                        })
                    .map(
                        elementPair ->
                            ImmutablePair.of(
                                elementPair.getLeft(),
                                schemaKeyRegexInfoTypes.stream()
                                    .filter(
                                        schemaKeyEntry ->
                                            elementPair
                                                .getLeft()
                                                .getName()
                                                .matches(schemaKeyEntry.getLeft()))
                                    .findAny()
                                    .map(ImmutablePair::getRight)
                                    .orElseGet(InfoType::getDefaultInstance)))
                    .filter(entry -> !entry.getRight().equals(InfoType.getDefaultInstance()))
                    .map(
                        fieldInfoType -> {
                          return Finding.newBuilder()
                              .setInfoType(fieldInfoType.getRight())
                              .setLocation(
                                  Location.newBuilder()
                                      .addContentLocations(
                                          ContentLocation.newBuilder()
                                              .setRecordLocation(
                                                  RecordLocation.newBuilder()
                                                      .setFieldId(fieldInfoType.getLeft())
                                                      .build())
                                              .build())
                                      .build())
                              .build();
                        })
                    .collect(toImmutableList());

            return InspectContentResponse.newBuilder()
                .setResult(InspectResult.newBuilder().addAllFindings(findings))
                .build();
          }
        };
      }
    };
  }

  private void assertProjectIdMatches(String parent) {
    assertThat(parent).isEqualTo(String.format("projects/%s", projectId));
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
