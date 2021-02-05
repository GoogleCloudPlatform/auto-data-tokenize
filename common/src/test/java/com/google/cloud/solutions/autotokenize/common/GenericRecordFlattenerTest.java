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

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public final class GenericRecordFlattenerTest {

  @RunWith(Parameterized.class)
  public static final class ValidInputOutputTests {

    @Parameter public String testConditionName;

    @Parameter(1)
    public String avroSchemaJsonFile;

    @Parameter(2)
    public String avroRecordJsonFile;

    @Parameter(3)
    public String expectedFlatRecordFile;

    @Parameters(name = "{0}")
    public static ImmutableList<Object[]> testingParameters() {
      return ImmutableList.<Object[]>builder()
          .add(
              new Object[] {
                "nonComplexFields",
                "avro_records/simple_field_avro_schema.json",
                "avro_records/simple_field_avro_record.json",
                "flat_records/simple_field_flat_record.textpb"
              })
          .add(
              new Object[] {
                "simpleArrayField",
                "avro_records/array_with_null_union_long_avro_schema.json",
                "avro_records/array_with_null_union_long_avro_record.json",
                "flat_records/array_with_null_union_long_flat_record.textpb"
              })
          .add(
              new Object[] {
                "recordArrayField",
                "avro_records/array_with_null_union_record_avro_schema.json",
                "avro_records/array_with_null_union_record_avro_record.json",
                "flat_records/array_with_null_union_record_flat_record.textpb"
              })
          .add(
              new Object[] {
                "unionWithArrayField",
                "avro_records/union_with_array_schema.json",
                "avro_records/union_with_array_record.json",
                "flat_records/union_with_array_flat_record.textpb"
              })
          .add(
              new Object[] {
                "userdata_avro",
                "avro_records/userdata_records/schema.json",
                "avro_records/userdata_records/record-1.json",
                "flat_records/userdata_avro/record-1.textpb"
              })
          .build();
    }

    @Test
    public void convert_valid() {
      GenericRecord testRecord =
          TestResourceLoader.classPath()
              .forAvro()
              .withSchemaFile(avroSchemaJsonFile)
              .loadRecord(avroRecordJsonFile);

      FlatRecord flatRecord = new GenericRecordFlattener().flatten(testRecord);

      assertThat(flatRecord)
          .isEqualTo(
              TestResourceLoader.classPath()
                  .forProto(FlatRecord.class)
                  .loadText(expectedFlatRecordFile));
    }
  }

  @RunWith(Parameterized.class)
  public static final class InvalidSchemaExceptionTests {

    @Parameter public String testConditionName;

    @Parameter(1)
    public String avroSchemaJsonFile;

    @Parameter(2)
    public String avroRecordJsonFile;

    @Parameter(3)
    public Class<? extends Exception> exceptionClass;

    @Parameter(4)
    public String expectedExceptionMessage;

    @Parameters(name = "{0} throws {3}")
    public static ImmutableList<Object[]> testingParameters() {
      return ImmutableList.<Object[]>builder()
          .add(
              new Object[] {
                "union3types",
                "avro_records/array_with_union_3types_avro_schema.json",
                "avro_records/array_with_union_3types_avro_record.json",
                UnsupportedOperationException.class,
                "Only nullable union with one type is supported. found [\"null\", \"long\","
                    + " \"string\"]"
              })
          .add(
              new Object[] {
                "unionFirstNonNullType",
                "avro_records/array_with_union_long_string_avro_schema.json",
                "avro_records/array_with_union_3types_avro_record.json",
                UnsupportedOperationException.class,
                "Only nullable union with one type is supported. found [\"long\", \"string\"]"
              })
          .add(
              new Object[] {
                "mapTypeField_",
                "avro_records/simple_map_field_avro_schema.json",
                "avro_records/simple_map_field_avro_record.json",
                UnsupportedOperationException.class,
                "Unsupported Type MAP at $.mapValue"
              })
          .build();
    }

    @Test
    public void convert_unsupportedSchema() {
      assertThat(
              assertThrows(
                  exceptionClass,
                  () ->
                      new GenericRecordFlattener()
                          .flatten(
                              TestResourceLoader.classPath()
                                  .forAvro()
                                  .withSchemaFile(avroSchemaJsonFile)
                                  .loadRecord(avroRecordJsonFile))))
          .hasMessageThat()
          .contains(expectedExceptionMessage);
    }
  }
}
