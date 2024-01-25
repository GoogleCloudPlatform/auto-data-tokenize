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

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.common.collect.ImmutableCollection;
import com.google.common.truth.extensions.proto.IterableOfProtosFluentAssertion;
import com.google.common.truth.extensions.proto.ProtoTruth;
import org.apache.beam.sdk.transforms.SerializableFunction;

@AutoValue
public abstract class FlatRecordsCheckerFn
    implements SerializableFunction<Iterable<FlatRecord>, Void> {

  abstract boolean isFlatKeySchemaPresent();

  abstract ImmutableCollection<FlatRecord> expectedRecords();

  public static FlatRecordsCheckerFn withExpectedRecords(
      ImmutableCollection<FlatRecord> expectedRecords) {
    return create(true, expectedRecords);
  }

  public FlatRecordsCheckerFn withoutFlatKeySchema() {
    return create(false, expectedRecords());
  }

  private static FlatRecordsCheckerFn create(
      boolean isFlatKeySchemaPresent, ImmutableCollection<FlatRecord> expectedRecords) {
    return new AutoValue_FlatRecordsCheckerFn(isFlatKeySchemaPresent, expectedRecords);
  }

  @Override
  public Void apply(Iterable<FlatRecord> input) {
    IterableOfProtosFluentAssertion<FlatRecord> assertion =
        ProtoTruth.assertThat(input).ignoringRepeatedFieldOrder();

    if (!isFlatKeySchemaPresent()) {
      assertion = assertion.ignoringFields(FlatRecord.FLAT_KEY_SCHEMA_FIELD_NUMBER);
    }

    assertion.containsExactlyElementsIn(expectedRecords());
    return null;
  }
}
