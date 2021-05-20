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

package com.google.cloud.solutions.autotokenize.testing;

import static com.google.common.truth.Truth.assertThat;

import org.apache.beam.sdk.transforms.SerializableFunction;

/** Utility Matcher for PAssert to check the number of items in a given PCollection. */
public class RecordsCountMatcher<T> implements SerializableFunction<Iterable<T>, Void> {

  private final int expectedRecordCount;

  public RecordsCountMatcher(int expectedRecordCount) {
    this.expectedRecordCount = expectedRecordCount;
  }

  public static <T> RecordsCountMatcher<T> hasRecordCount(int expectedRecordCount) {
    return new RecordsCountMatcher<>(expectedRecordCount);
  }

  @Override
  public Void apply(Iterable<T> input) {
    assertThat(input).hasSize(expectedRecordCount);
    return null;
  }
}
