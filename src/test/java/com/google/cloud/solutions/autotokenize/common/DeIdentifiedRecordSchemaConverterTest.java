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

import static com.google.cloud.solutions.autotokenize.common.DeIdentifiedRecordSchemaConverter.withOriginalSchema;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.solutions.autotokenize.testing.JsonSubject;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import org.apache.avro.Schema;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public final class DeIdentifiedRecordSchemaConverterTest {

  private static final ImmutableList<String> DUMMY_ENCRYPT_COLUMN = ImmutableList.of("xyz");

  @RunWith(JUnit4.class)
  public static final class SingleTests {

    @Test
    public void build_usesSystemClockUtc() {
      assertThat(
              withOriginalSchema(
                      TestResourceLoader.classPath()
                          .forAvro()
                          .asSchema("avro_records/union_with_array_schema.json"))
                  .withEncryptColumnKeys(DUMMY_ENCRYPT_COLUMN)
                  .getClock())
          .isEqualTo(Clock.systemUTC());
    }

    @Test
    public void build_nullEncryptedColumns_throwsException() {

      Schema testSchema =
          TestResourceLoader.classPath()
              .forAvro()
              .asSchema("avro_records/union_with_array_schema.json");

      var builderWithNoEncryptedColumns = withOriginalSchema(testSchema);

      IllegalStateException illegalStateException =
          assertThrows(IllegalStateException.class, builderWithNoEncryptedColumns::updatedSchema);

      assertThat(illegalStateException)
          .hasMessageThat()
          .contains("schema and encrypt columns can't be null or empty");
    }

    @Test
    public void build_emptyEncryptedColumns_throwsException() {

      Schema testSchema =
          TestResourceLoader.classPath()
              .forAvro()
              .asSchema("avro_records/union_with_array_schema.json");

      var emptyColumnsBuilder =
          withOriginalSchema(testSchema).withEncryptColumnKeys(ImmutableList.of());

      IllegalStateException illegalStateException =
          assertThrows(IllegalStateException.class, emptyColumnsBuilder::updatedSchema);

      assertThat(illegalStateException)
          .hasMessageThat()
          .contains("schema and encrypt columns can't be null or empty");
    }

    @Test
    public void build_noSchema_throwsNullPointerException() {
      NullPointerException nullPointerException =
          assertThrows(NullPointerException.class, () -> withOriginalSchema(null));

      assertThat(nullPointerException).hasMessageThat().contains("original schema can't be null");
    }

    @Test
    public void build_noSchemaNoColumn_throwsException() {

      var schemaConverter =
          withOriginalSchema(
              TestResourceLoader.classPath()
                  .forAvro()
                  .asSchema("avro_records/union_with_array_schema.json"));

      IllegalStateException illegalStateException =
          assertThrows(IllegalStateException.class, schemaConverter::updatedSchema);

      assertThat(illegalStateException)
          .hasMessageThat()
          .contains("schema and encrypt columns can't be null or empty");
    }
  }

  @RunWith(Parameterized.class)
  public static final class ValidParameterizedTests {

    private final Schema inputSchema;
    private final Schema expectedSchema;
    private final ImmutableSet<String> encryptColumnNames;
    private final Clock fixedTestClock;

    public ValidParameterizedTests(
        String testConditionName,
        String simulatedExecutionTimestamp,
        String inputSchemaFile,
        String encryptedSchemaFile,
        ImmutableSet<String> encryptColumnNames) {
      this.fixedTestClock = Clock.fixed(Instant.parse(simulatedExecutionTimestamp), ZoneOffset.UTC);
      this.inputSchema = TestResourceLoader.classPath().forAvro().asSchema(inputSchemaFile);
      this.expectedSchema = TestResourceLoader.classPath().forAvro().asSchema(encryptedSchemaFile);
      this.encryptColumnNames = encryptColumnNames;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> schemaData() {
      return ImmutableList.<Object[]>builder()
          .add(
              new Object[] {
                "encrypted_column_in_union_record_field",
                "2020-10-21T00:30:15Z",
                "avro_records/array_with_null_union_record_avro_schema.json",
                "avro_records/array_with_null_union_record_avro_encrypted_schema.json",
                ImmutableSet.of("$.kylosample.name", "$.kylosample.contacts.contact.number")
              })
          .add(
              new Object[] {
                "encrypted_column_in_union_long_field",
                "2020-10-21T00:40:15Z",
                "avro_records/array_with_null_union_long_avro_schema.json",
                "avro_records/array_with_null_union_long_avro_encrypted_schema.json",
                ImmutableSet.of("$.kylosample.name", "$.kylosample.nums")
              })
          .add(
              new Object[] {
                "fields_with_aliases_copied_valid",
                "2020-10-21T00:50:15Z",
                "avro_records/nullable_encryption_column_schema.json",
                "avro_records/nullable_encryption_column_encrypted_schema.json",
                ImmutableSet.of("$.kylosample.name", "$.kylosample.contacts.contact.number")
              })
          .add(
              new Object[] {
                "copies_user_defined_props",
                "2020-10-21T01:00:15Z",
                "avro_records/avroSchema_with_user_defined_props.json",
                "avro_records/avroSchema_with_user_defined_props_encrypted.json",
                ImmutableSet.of("$.kylosample.name", "$.kylosample.contacts.contact.number")
              })
          .add(
              new Object[] {
                "union_with_all_types",
                "2020-10-21T01:00:15Z",
                "avro_records/union_with_all_types_avro_schema.json",
                "avro_records/union_with_all_types_avro_encrypted_schema.json",
                ImmutableSet.of(
                    "$.union_all_test.union_with_enum",
                    "$.union_all_test.union_with_boolean",
                    "$.union_all_test.union_with_long",
                    "$.union_all_test.union_with_int",
                    "$.union_all_test.union_with_string",
                    "$.union_all_test.union_with_float",
                    "$.union_all_test.union_with_double",
                    "$.union_all_test.union_with_fixed",
                    "$.union_all_test.union_with_bytes",
                    "$.union_all_test.union_with_record.custom_record.custom_field"
                        + ".custom_field_record.second_level_field_string")
              })
          .add(
              new Object[] {
                "union_with_simple_type_array: string type for encrypted field",
                "2020-10-21T01:00:15Z",
                "avro_records/union_with_array_long_schema.json",
                "avro_records/union_with_array_long_encrypted_schema.json",
                ImmutableSet.of("$.kylosample.cc")
              })
          .add(
              new Object[] {
                "union_with_simple_type_array: string type for encrypted field",
                "2020-10-21T01:00:15Z",
                "avro_records/union_with_array_schema.json",
                "avro_records/union_with_array_encrypted_schema.json",
                ImmutableSet.of("$.kylosample.cc")
              })
          .add(
              new Object[] {
                "contact_schema",
                "2020-10-21T01:00:15Z",
                "avro_records/contacts_schema/person_name_union_null_long_contact_schema.json",
                "avro_records/contacts_schema/person_name_union_null_long_contact_encrypted_schema.json",
                ImmutableSet.of("$.contact_records.contacts.contact.number")
              })
          .add(
              new Object[] {
                "contact_schema",
                "2020-10-21T01:00:15Z",
                "avro_records/contact_records_with_namespace/bq_contacts_schema.json",
                "avro_records/contact_records_with_namespace/bq_contacts_schema_encrypted.json",
                ImmutableSet.of("$.Root.contact.root.Contact.nums.root.contact.Nums.number")
              })
          .add(
              new Object[] {
                "nyc_taxi_parquet_avro_schema",
                "2020-10-21T01:00:15Z",
                "avro_records/nyc_taxi_avro_schema.json",
                "avro_records/nyc_taxi_avro_schema_encrypted.json",
                ImmutableSet.of(
                    "$.schema.vendor_id", "$.schema.dropoff_latitude", "$.schema.dropoff_longitude")
              })
          .build();
    }

    @Test
    public void updatedSchema_valid() {
      Schema updatedSchema =
          withOriginalSchema(inputSchema)
              .withEncryptColumnKeys(encryptColumnNames)
              .withClock(fixedTestClock)
              .updatedSchema();

      JsonSubject.assertThat(updatedSchema).isEqualTo(expectedSchema);
    }
  }

  @RunWith(Parameterized.class)
  public static final class ExceptionParameterizedTests {

    private final Schema testSchema;
    private final String errorMessage;
    private final Class<? extends Exception> exceptionClass;

    public ExceptionParameterizedTests(
        String testCondition,
        String schemaLocation,
        String errorMessage,
        Class<? extends Exception> exceptionClass) {
      this.testSchema = TestResourceLoader.classPath().forAvro().asSchema(schemaLocation);
      this.errorMessage = errorMessage;
      this.exceptionClass = exceptionClass;
    }

    @Parameters(name = "{0} throws {2}")
    public static ImmutableList<Object[]> exceptionParameters() {
      return ImmutableList.<Object[]>builder()
          .add(
              new Object[] {
                "union_3_fields",
                "avro_records/array_with_union_3types_avro_schema.json",
                "Union can contain max of two types. with first being null",
                UnsupportedOperationException.class
              })
          .add(
              new Object[] {
                "map_field",
                "avro_records/simple_map_field_avro_schema.json",
                "Type not supported in Schema - MAP",
                UnsupportedOperationException.class
              })
          .add(
              new Object[] {
                "union_field_with_map",
                "avro_records/union_with_map_schema.json",
                "Union of Union/Map is invalid schema",
                UnsupportedOperationException.class
              })
          .build();
    }

    @Test
    public void updatedSchema_throwsException() {

      Exception osoEx =
          assertThrows(
              exceptionClass,
              () ->
                  withOriginalSchema(testSchema)
                      .withEncryptColumnKeys(DUMMY_ENCRYPT_COLUMN)
                      .updatedSchema());

      assertThat(osoEx).hasMessageThat().contains(errorMessage);
    }
  }
}
