/*
 * Copyright 2020 Google LLC
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

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class RecordUnFlattenerTest {

  private final Schema testAvroSchema;
  private final FlatRecord testFlatRecord;
  private final GenericRecord expectedRecord;

  public RecordUnFlattenerTest(
      String testCondition,
      String testAvroSchemaPath,
      String expectedAvroRecord,
      String flatFilePath) {
    this.testAvroSchema = TestResourceLoader.classPath().forAvro().asSchema(testAvroSchemaPath);
    this.testFlatRecord =
        TestResourceLoader.classPath().forProto(FlatRecord.class).loadText(flatFilePath);
    this.expectedRecord =
        TestResourceLoader.classPath()
            .forAvro()
            .withSchemaFile(testAvroSchemaPath)
            .loadRecord(expectedAvroRecord);
  }

  @Parameters(name = "{0}")
  public static ImmutableList<Object[]> testingParameters() {
    return ImmutableList.<Object[]>builder()
        .add(
            new Object[]{
                "nonComplexFields",
                "avro_records/simple_field_avro_schema.json",
                "avro_records/simple_field_avro_record.json",
                "flat_records/simple_field_flat_record.textpb"
            })
        .add(
            new Object[]{
                "simpleArrayField",
                "avro_records/array_with_null_union_long_avro_schema.json",
                "avro_records/array_with_null_union_long_avro_record.json",
                "flat_records/array_with_null_union_long_flat_record.textpb"
            })
        .add(
            new Object[]{
                "recordArrayField",
                "avro_records/array_with_null_union_record_avro_schema.json",
                "avro_records/array_with_null_union_record_avro_record.json",
                "flat_records/array_with_null_union_record_flat_record.textpb"
            })
        .add(
            new Object[]{
                "unionWithArrayField",
                "avro_records/union_with_array_schema.json",
                "avro_records/union_with_array_record.json",
                "flat_records/union_with_array_flat_record.textpb"
            })
        .build();
  }

  @Test
  public void unflatten_valid() {
    GenericRecord unflattenedRecord =
        RecordUnflattener.forSchema(testAvroSchema).unflatten(testFlatRecord);

    assertThat(unflattenedRecord).isEqualTo(expectedRecord);
  }
}
