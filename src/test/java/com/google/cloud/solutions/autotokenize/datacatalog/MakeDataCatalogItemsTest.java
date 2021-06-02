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

package com.google.cloud.solutions.autotokenize.datacatalog;


import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.InspectionReport;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.JdbcConfiguration;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.UpdatableDataCatalogItems;
import com.google.cloud.solutions.autotokenize.testing.CompareProtoIgnoringRepeatedFieldOrder;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.util.Timestamps;
import java.text.ParseException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.avro.Schema;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public final class MakeDataCatalogItemsTest {

  private static final Clock FIXED_TEST_CLOCK =
      Clock.fixed(ZonedDateTime.parse("2021-05-18T12:16:51.767Z").toInstant(), ZoneOffset.UTC);

  @RunWith(Parameterized.class)
  public static final class ParameterizedMakeDataCatalogItemsTest {

    @Rule public TestPipeline testPipeline = TestPipeline.create();

    private final Schema inputAvroSchema;
    private final UpdatableDataCatalogItems expectedUpdateItems;
    private final ImmutableList<ColumnInformation> sensitiveColumnsInfo;

    // Pipeline Options
    private final SourceType sourceType;
    private final String inputPattern;
    private final JdbcConfiguration jdbcConfiguration;

    public ParameterizedMakeDataCatalogItemsTest(
        String testConditionName,
        String inputAvroSchemaFile,
        String expectedUpdateItemsFile,
        ImmutableList<String> sensitiveColumnsInfoFiles,
        SourceType sourceType,
        String inputPattern,
        @Nullable JdbcConfiguration jdbcConfiguration) {
      this.inputAvroSchema = TestResourceLoader.classPath().forAvro().asSchema(inputAvroSchemaFile);

      this.expectedUpdateItems =
          (expectedUpdateItemsFile == null)
              ? null
              : TestResourceLoader.classPath()
                  .forProto(UpdatableDataCatalogItems.class)
                  .loadJson(expectedUpdateItemsFile);

      this.sensitiveColumnsInfo =
          TestResourceLoader.classPath()
              .forProto(ColumnInformation.class)
              .loadAllJsonFiles(sensitiveColumnsInfoFiles);

      this.sourceType = sourceType;
      this.inputPattern = inputPattern;
      this.jdbcConfiguration = jdbcConfiguration;
    }

    @Test
    public void expand_valid() throws ParseException {
      var report =
          InspectionReport.newBuilder()
              .setTimestamp(Timestamps.parse("2021-05-18T12:16:51.767Z"))
              .setSourceType(sourceType)
              .setInputPattern(inputPattern)
              .setAvroSchema(inputAvroSchema.toString())
              .addAllColumnReport(sensitiveColumnsInfo);

      if (jdbcConfiguration != null) {
        report.setJdbcConfiguration(jdbcConfiguration);
      }

      var items =
          testPipeline
              .apply(
                  "CreateSampleDlpFindings",
                  Create.of(report.build()).withCoder(ProtoCoder.of(InspectionReport.class)))
              .apply(
                  MakeDataCatalogItems.create(
                      "projects/my-project-id/locations/asia-singapore1/tagTemplates/my_test_template"));

      if (expectedUpdateItems == null) {
        PAssert.that(items).empty();
      } else {
        PAssert.thatSingleton(items)
            .satisfies(new CompareProtoIgnoringRepeatedFieldOrder<>(expectedUpdateItems));
      }

      testPipeline.run();
    }

    private void applyCorrectUpdatableItemsAssertion(
        PCollection<UpdatableDataCatalogItems> items) {}

    @Parameters(name = "{0}")
    public static ImmutableList<Object[]> testParameters() {
      return ImmutableList.<Object[]>builder()
          .add(
              new Object[] {
                "Source:AVRO - Fileset Entry + Tags",
                /*inputAvroSchemaFile=*/ "catalog_schema_items/nested_repeated_row_avro_schema.json",
                /*expectedUpdateItemsFile=*/ "catalog_schema_items/avro_updatable_items.json",
                /*sensitiveColumnInfoFiles=*/ ImmutableList.of(
                    "catalog_schema_items/nested-contact_number-00000-of-00001.json",
                    "catalog_schema_items/col-topLevelRecord-person_name-00000-of-00001.json"),
                /*sourceType=*/ SourceType.AVRO,
                /*inputPattern=*/ "gs://bucket-id/path/to/files/file-prefix-*.avro",
                /*JdbcConfiguration=*/ null
              })
          .add(
              new Object[] {
                "Source:JDBC_TABLE - Custom Entry + Tags",
                /*inputAvroSchemaFile=*/ "catalog_schema_items/flat_row_avro_schema.json",
                /*expectedUpdateItemsFile=*/ "catalog_schema_items/jdbc_updatedable_items.json",
                /*sensitiveColumnInfoFiles=*/ ImmutableList.of(
                    "catalog_schema_items/col-topLevelRecord-contact_number-00000-of-00001.json",
                    "catalog_schema_items/col-topLevelRecord-person_name-00000-of-00001.json"),
                /*sourceType=*/ SourceType.JDBC_TABLE,
                /*inputPattern=*/ "SimpleFlatRecords",
                /*JdbcConfiguration=*/ JdbcConfiguration.newBuilder()
                    .setDriverClassName("com.google.databaseType.Driver")
                    .setConnectionUrl(
                        "jdbc:mysql:///dlp_test_database?cloudSqlInstance=auto-dlp%3Aasia-southeast1%3Adlp-test-instance&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=root%401234")
                    .build()
              })
          .add(
              new Object[] {
                "Source:BIGQUERY_TABLE - Only Tags, No Entry",
                /*inputAvroSchemaFile=*/ "catalog_schema_items/nested_repeated_field_avro_schema.json",
                /*expectedUpdateItemsFile=*/ "catalog_schema_items/bigquery_nested_repeated_field_updatable_items.json",
                /*sensitiveColumnInfoFiles=*/ ImmutableList.of(
                    "catalog_schema_items/nested_repeated_field_col-__root__-contact-__s_0-name-00000-of-00001.json",
                    "catalog_schema_items/nested_repeated_field_col-__root__-contact-__s_0-nums-__s_1-number-00000-of-00001.json"),
                /*sourceType=*/ SourceType.BIGQUERY_TABLE,
                /*inputPattern=*/ "gcpProject:dataset.BigQueryContactsTable",
                /*JdbcConfiguration=*/ null
              })
          .add(
              new Object[] {
                "Source:BIGQUERY_QUERY - No updatable items",
                /*inputAvroSchemaFile=*/ "catalog_schema_items/nested_repeated_row_avro_schema.json",
                /*expectedUpdateItemsFile=*/ null,
                /*sensitiveColumnInfoFiles=*/ ImmutableList.of(
                    "catalog_schema_items/nested-contact_number-00000-of-00001.json",
                    "catalog_schema_items/col-topLevelRecord-person_name-00000-of-00001.json"),
                /*sourceType=*/ SourceType.BIGQUERY_QUERY,
                /*inputPattern=*/ "SELECT * FROM `dataset.BigquerySimpleFlatRecords`;",
                /*JdbcConfiguration=*/ null
              })
          .build();
    }
  }
}
