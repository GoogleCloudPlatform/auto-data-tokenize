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

import static com.google.cloud.solutions.autotokenize.testing.FlatRecordsCheckerFn.withExpectedRecords;
import static com.google.cloud.solutions.autotokenize.testing.RandomGenericRecordGenerator.generateGenericRecords;
import static com.google.cloud.solutions.autotokenize.testing.RecordsCountMatcher.hasRecordCount;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.JdbcConfiguration;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.common.CsvIO.CsvRow;
import com.google.cloud.solutions.autotokenize.testing.RandomGenericRecordGenerator;
import com.google.cloud.solutions.autotokenize.testing.TestCsvFileGenerator;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.cloud.solutions.autotokenize.testing.stubs.secretmanager.ConstantSecretVersionValueManagerServicesStub;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import com.google.common.truth.Correspondence;
import com.google.common.truth.Correspondence.BinaryPredicate;
import com.google.privacy.dlp.v2.Value;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import junit.framework.AssertionFailedError;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.Statement;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

@RunWith(Enclosed.class)
public final class TransformingReaderTest {

  @RunWith(Parameterized.class)
  public static final class BigQueryReaderTest {

    private transient PipelineOptions options;
    private final transient TemporaryFolder testFolder = new TemporaryFolder();
    private transient TestPipeline readPipeline;

    @Rule
    public final transient TestRule folderThenPipeline =
        new TestRule() {
          @Override
          public Statement apply(final Statement base, final Description description) {
            // We need to set up the temporary folder, and then set up the TestPipeline based on the
            // chosen folder. Unfortunately, since rule evaluation order is unspecified and
            // unrelated
            // to field order, and is separate from construction, that requires manually creating
            // this
            // TestRule.
            Statement withPipeline =
                new Statement() {
                  @Override
                  public void evaluate() throws Throwable {
                    options = TestPipeline.testingPipelineOptions();
                    options.as(BigQueryOptions.class).setProject(testTableRef.getProjectId());
                    options
                        .as(BigQueryOptions.class)
                        .setTempLocation(testFolder.getRoot().getAbsolutePath());
                    readPipeline = TestPipeline.fromOptions(options);
                    readPipeline.apply(base, description).evaluate();
                  }
                };
            return testFolder.apply(withPipeline, description);
          }
        };

    @Rule public transient ExpectedException thrown = ExpectedException.none();

    private final FakeDatasetService fakeDatasetService = new FakeDatasetService();
    private final FakeJobService fakeJobService = new FakeJobService();
    private final FakeBigQueryServices fakeBqServices =
        new FakeBigQueryServices()
            .withDatasetService(fakeDatasetService)
            .withJobService(fakeJobService);

    private final TableReference testTableRef;
    private final List<TableRow> testRows;
    private final TableSchema testTableSchema;
    private final ImmutableList<FlatRecord> expectedRecords;
    private final Table testTable;

    public BigQueryReaderTest(
        String testOptionsName,
        TableReference testTableRef,
        List<TableRow> testRows,
        TableSchema testTableSchema,
        ImmutableList<FlatRecord> expectedRecords) {
      this.testTableRef = testTableRef;
      this.testRows = testRows;
      this.testTableSchema = testTableSchema;
      this.expectedRecords = expectedRecords;
      this.testTable = new Table().setTableReference(testTableRef).setSchema(testTableSchema);
    }

    @Parameters(name = "{0}")
    public static ImmutableList<Object[]> testingParameter() {
      return ImmutableList.<Object[]>builder()
          .add(
              new Object[] {
                "simple_schema_two_rows",
                new TableReference()
                    .setProjectId("test-project")
                    .setDatasetId("sample_dataset")
                    .setTableId("TestTable"),
                ImmutableList.of(
                    new TableRow().set("first_record", 1L).set("second_record", "some string 1"),
                    new TableRow().set("first_record", 2L).set("second_record", "some string 2")),
                new TableSchema()
                    .setFields(
                        ImmutableList.of(
                            new TableFieldSchema().setName("first_record").setType("INTEGER"),
                            new TableFieldSchema().setName("second_record").setType("STRING"))),
                ImmutableList.of(
                    FlatRecord.newBuilder()
                        .putFlatKeySchema("$.first_record.long", "$.first_record")
                        .putFlatKeySchema("$.second_record.string", "$.second_record")
                        .putValues(
                            "$.first_record.long", Value.newBuilder().setIntegerValue(1).build())
                        .putValues(
                            "$.second_record.string",
                            Value.newBuilder().setStringValue("some string 1").build())
                        .build(),
                    FlatRecord.newBuilder()
                        .putFlatKeySchema("$.first_record.long", "$.first_record")
                        .putFlatKeySchema("$.second_record.string", "$.second_record")
                        .putValues(
                            "$.first_record.long", Value.newBuilder().setIntegerValue(2).build())
                        .putValues(
                            "$.second_record.string",
                            Value.newBuilder().setStringValue("some string 2").build())
                        .build())
              })
          .build();
    }

    @Before
    public void setUpTestDatasetAndTable() throws IOException, InterruptedException {
      FakeDatasetService.setUp();
      fakeDatasetService.createDataset(
          testTableRef.getProjectId(), testTableRef.getDatasetId(), "", "", null);

      fakeDatasetService.createTable(testTable);

      fakeDatasetService.insertAll(testTableRef, testRows, null);
    }

    @Test
    public void expand_bigqueryTable_valid() {

      var recordsTag = new TupleTag<FlatRecord>();
      var schemaTag = new TupleTag<String>();
      var recordsAndSchema =
          readPipeline.apply(
              TransformingReader.forSourceType(SourceType.BIGQUERY_TABLE)
                  .withBigQueryTestServices(fakeBqServices)
                  .from(
                      String.format(
                          "%s:%s.%s",
                          testTableRef.getProjectId(),
                          testTableRef.getDatasetId(),
                          testTableRef.getTableId()))
                  .withRecordsTag(recordsTag)
                  .withAvroSchemaTag(schemaTag));

      PAssert.that(recordsAndSchema.get(schemaTag)).satisfies(hasRecordCount(1));
      PAssert.that(recordsAndSchema.get(recordsTag))
          .satisfies(new ValidateSchemaKeyWithTempQueryTableFn(expectedRecords));

      readPipeline.run().waitUntilFinish();
    }

    @Test
    public void expand_bigqueryQuery_valid() throws IOException {
      var encodeQueryString = buildEncodedQueryString();

      fakeJobService.expectDryRunQuery(
          testTableRef.getProjectId(),
          encodeQueryString,
          new JobStatistics()
              .setQuery(
                  new JobStatistics2()
                      .setTotalBytesProcessed(1000L)
                      .setReferencedTables(ImmutableList.of(testTableRef))));

      var recordsTag = new TupleTag<FlatRecord>();
      var schemaTag = new TupleTag<String>();
      var recordsAndSchema =
          readPipeline.apply(
              TransformingReader.forSourceType(SourceType.BIGQUERY_QUERY)
                  .withBigQueryTestServices(fakeBqServices)
                  .from(encodeQueryString)
                  .withRecordsTag(recordsTag)
                  .withAvroSchemaTag(schemaTag));

      PAssert.that(recordsAndSchema.get(schemaTag)).satisfies(hasRecordCount(1));
      PAssert.that(recordsAndSchema.get(recordsTag))
          .satisfies(new ValidateSchemaKeyWithTempQueryTableFn(expectedRecords));

      readPipeline.run().waitUntilFinish();
    }

    private static class ValidateSchemaKeyWithTempQueryTableFn
        implements SerializableFunction<Iterable<FlatRecord>, Void> {

      private final ImmutableList<FlatRecord> expectedRecords;

      public ValidateSchemaKeyWithTempQueryTableFn(ImmutableList<FlatRecord> expectedRecords) {
        this.expectedRecords = expectedRecords;
      }

      @Override
      public Void apply(Iterable<FlatRecord> input) {
        assertThat(input)
            .comparingElementsUsing(
                Correspondence.from(
                    new CompareFlatRecordsWithTempQueryTable(), "Compare without tempTableName"))
            .containsExactlyElementsIn(expectedRecords);

        return null;
      }
    }

    private static class CompareFlatRecordsWithTempQueryTable
        implements BinaryPredicate<FlatRecord, FlatRecord> {

      @Override
      public boolean apply(
          AutoTokenizeMessages.FlatRecord actual, AutoTokenizeMessages.FlatRecord expected) {

        if ((actual == null && expected != null) || (actual != null && expected == null)) {
          return false;
        } else if (actual == null && expected == null) {
          return true;
        }

        var expectedSchemaKeys = expected.getFlatKeySchemaMap();

        var schemaKeyMatches =
            actual.getFlatKeySchemaMap().entrySet().stream()
                .map(
                    entry -> {
                      if (!expectedSchemaKeys.containsKey(entry.getKey())) {
                        return false;
                      }

                      var expectedSchemaKeyRegex =
                          String.format(
                              "^\\$\\.org\\.apache\\.beam\\.sdk\\.io\\.gcp\\.bigquery.\\w+.%s$",
                              expectedSchemaKeys.get(entry.getKey()).replaceAll("^\\$\\.", ""));

                      return entry.getValue().matches(expectedSchemaKeyRegex);
                    })
                .reduce(Boolean::logicalAnd)
                .orElse(false);

        return schemaKeyMatches && actual.getValuesMap().equals(expected.getValuesMap());
      }
    }

    private String buildEncodedQueryString() throws IOException {
      KvCoder<String, List<TableRow>> coder =
          KvCoder.of(StringUtf8Coder.of(), ListCoder.of(TableRowJsonCoder.of()));

      var baos = new ByteArrayOutputStream();
      coder.encode(KV.of(BigQueryHelpers.toJsonString(testTable), testRows), baos);
      return Base64.getEncoder().encodeToString(baos.toByteArray());
    }
  }

  @RunWith(Parameterized.class)
  public static final class JdbcReaderTest {

    @Rule public transient TestPipeline testPipeline = TestPipeline.create();

    private final JdbcDatabaseContainer<?> databaseContainer;

    private final String testConditionName;
    private final String initScriptFile;
    private final String tableName;
    private final JdbcConfiguration jdbcConfiguration;
    private final ImmutableList<FlatRecord> expectedRecords;
    private final String testDatabaseName;
    private final ConstantSecretVersionValueManagerServicesStub secretsStub;

    public JdbcReaderTest(
        String testConditionName,
        String initScriptFile,
        String tableName,
        JdbcConfiguration jdbcConfiguration,
        ImmutableList<FlatRecord> expectedRecords) {
      this.testConditionName = testConditionName;
      this.initScriptFile = initScriptFile;
      this.tableName = tableName;
      this.jdbcConfiguration = jdbcConfiguration;
      this.expectedRecords = expectedRecords;
      this.testDatabaseName = ("test_" + new Random().nextLong()).replaceAll("-", "");
      this.secretsStub =
          ConstantSecretVersionValueManagerServicesStub.of("id/to/secrets/key/version", "");

      databaseContainer =
          new MySQLContainer<>("mysql:8.0.24")
              .withDatabaseName(testDatabaseName)
              .withUsername("root")
              .withPassword("")
              .withInitScript(initScriptFile);
      databaseContainer.start();
    }

    @After
    public void tearDownDatabase() {
      if (databaseContainer != null) {
        databaseContainer.stop();
      }
    }

    @Test
    public void expand_jdbc_validFlatRecords() {

      var recordTag = new TupleTag<FlatRecord>();
      var schemaTag = new TupleTag<String>();

      var recordsAndSchema =
          testPipeline.apply(
              TransformingReader.forSourceType(SourceType.JDBC_TABLE)
                  .from(tableName)
                  .withJdbcConfiguration(
                      jdbcConfiguration.toBuilder()
                          .setConnectionUrl(getCompleteDbConnectionString())
                          .build())
                  .withSecretsClient(SecretsClient.withSecretsStub(secretsStub))
                  .withRecordsTag(recordTag)
                  .withAvroSchemaTag(schemaTag));

      PAssert.that(recordsAndSchema.get(recordTag)).satisfies(withExpectedRecords(expectedRecords));
      PAssert.that(recordsAndSchema.get(schemaTag)).satisfies(hasRecordCount(1));
      testPipeline.run();
    }

    @Parameters(name = "{0}")
    public static ImmutableList<Object[]> testingParameters() {
      return ImmutableList.<Object[]>builder()
          .add(
              new Object[] {
                "SimpleFlatRecord",
                "db_init_scripts/simple_flat_records.sql",
                "SimpleFlatRecords",
                JdbcConfiguration.newBuilder()
                    .setDriverClassName("com.mysql.cj.jdbc.Driver")
                    .setUserName("root")
                    .setPasswordSecretsKey("id/to/secrets/key/version")
                    .build(),
                TestResourceLoader.classPath()
                    .forProto(FlatRecord.class)
                    .loadAllTextFiles(
                        "jdbc_flatrecords/record_1.textpb",
                        "jdbc_flatrecords/record_2.textpb",
                        "jdbc_flatrecords/record_3.textpb")
              },
              new Object[] {
                "TableWithTimeFields",
                "db_init_scripts/table_with_timefields_records.sql",
                "TableWithTimeFields",
                JdbcConfiguration.newBuilder()
                    .setDriverClassName("com.mysql.cj.jdbc.Driver")
                    .setUserName("root")
                    .setPassword("")
                    .build(),
                TestResourceLoader.classPath()
                    .forProto(FlatRecord.class)
                    .loadAllTextFiles("jdbc_flatrecords/date_time_fields_flatrecords.textpb")
              })
          .build();
    }

    private String getCompleteDbConnectionString() {
      return String.format("%s?user=%s&password=%s", databaseContainer.getJdbcUrl(), "root", "");
    }
  }

  @RunWith(Parameterized.class)
  public static final class AvroParquetReaderTest {

    private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

    @Rule public transient TestPipeline writePipeline = TestPipeline.create();

    @Rule public transient TestPipeline readPipeline = TestPipeline.create();

    @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final SourceType fileSourceType;

    private final int expectedRecordsCount;

    private final String resourceFile;

    private final String expectedSchemaFile;

    private static final Schema DEFAULT_TEST_SCHEMA = RandomGenericRecordGenerator.SCHEMA;

    private String testFilePattern;

    @Parameters(name = "{0} file contains {1} records")
    public static Collection<Object[]> differentFileTypes() {
      return ImmutableList.<Object[]>builder()
          .add(new Object[] {SourceType.AVRO, 1000, null, null})
          .add(new Object[] {SourceType.PARQUET, 1000, null, null})
          .add(
              new Object[] {
                SourceType.AVRO,
                2,
                "avro_records/bq_exported_tables/table_with_date_and_timestamp.avro",
                "avro_records/bq_exported_tables/table_with_data_and_timestamp_schema.json"
              })
          .build();
    }

    public AvroParquetReaderTest(
        SourceType fileSourceType,
        int expectedRecordsCount,
        String resourceFile,
        String expectedSchemaFile) {
      this.fileSourceType = fileSourceType;
      this.expectedRecordsCount = expectedRecordsCount;
      this.resourceFile = resourceFile;
      this.expectedSchemaFile = expectedSchemaFile;
    }

    @Before
    public void makeInputFileAccessible() throws IOException {
      var testInputFolder = temporaryFolder.newFolder();
      testInputFolder.mkdirs();

      if (resourceFile != null) {
        testFilePattern =
            TestResourceLoader.classPath()
                .copyTo(testInputFolder)
                .createFileTestCopy(resourceFile)
                .getAbsolutePath();
        return;
      }

      generateTestRecordsFile(testInputFolder.getAbsolutePath());
    }

    public void generateTestRecordsFile(String targetFolder) {
      logger.atInfo().log("Writing records to: %s", targetFolder);

      writePipeline
          .apply(
              Create.of(generateGenericRecords(expectedRecordsCount))
                  .withCoder(AvroCoder.of(DEFAULT_TEST_SCHEMA)))
          .apply(
              FileIO.<GenericRecord>write()
                  .via(fileTypeSpecificSink(DEFAULT_TEST_SCHEMA))
                  .to(targetFolder));

      writePipeline.run().waitUntilFinish();

      testFilePattern = targetFolder + "/*";
    }

    @Test
    public void expand_providedFile_recordCountMatched() {
      var recordsTag = new TupleTag<FlatRecord>();
      var schemaTag = new TupleTag<String>();

      var recordsAndSchemaTuple =
          readPipeline.apply(
              TransformingReader.forSourceType(fileSourceType)
                  .from(testFilePattern)
                  .withRecordsTag(recordsTag)
                  .withAvroSchemaTag(schemaTag));

      var records = recordsAndSchemaTuple.get(recordsTag);
      var schema = recordsAndSchemaTuple.get(schemaTag);

      PAssert.that(records).satisfies(hasRecordCount(expectedRecordsCount));
      PAssert.that(schema).satisfies(hasRecordCount(1));
      PAssert.that(schema)
          .containsInAnyOrder(
              ImmutableList.of(
                  (expectedSchemaFile != null)
                      ? TestResourceLoader.classPath()
                          .forAvro()
                          .asSchema(expectedSchemaFile)
                          .toString()
                      : DEFAULT_TEST_SCHEMA.toString()));

      readPipeline.run().waitUntilFinish();
    }

    private FileIO.Sink<GenericRecord> fileTypeSpecificSink(Schema schema) {
      switch (fileSourceType) {
        case AVRO:
          return AvroIO.sink(schema);

        case PARQUET:
          return ParquetIO.sink(schema);
      }

      throw new UnsupportedOperationException("not supported for type: " + fileSourceType.name());
    }
  }

  @RunWith(JUnit4.class)
  public static final class CsvReaderTest {

    @Rule public TestPipeline testPipeline = TestPipeline.create();

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private List<List<String>> testCsvRecords;

    private String inputFile;

    TupleTag<FlatRecord> recordsTag = new TupleTag<>();
    TupleTag<String> schemaTag = new TupleTag<>();

    @Before
    public void makeTestCsvFile() throws IOException {
      var testInputFolder = temporaryFolder.newFolder().getAbsolutePath();
      inputFile = testInputFolder + "/input.csv";

      testCsvRecords = TestCsvFileGenerator.create().writeRandomRecordsToFile(inputFile);
    }

    @Test
    public void expand_simpleCsv_valid() {

      var recordsAndSchema =
          testPipeline.apply(
              "LoadCsvFile",
              TransformingReader.forSourceType(SourceType.CSV_FILE)
                  .from(inputFile)
                  .withRecordsTag(recordsTag)
                  .withAvroSchemaTag(schemaTag));

      var schema = recordsAndSchema.get(schemaTag).apply(Sample.any(1));

      var colValues =
          recordsAndSchema
              .get(recordsTag)
              .apply("MakeCsv", MapElements.via(CsvRowFlatRecordConvertors.flatRecordToCsvRowFn()))
              .apply("MakeStringList", MapElements.via(new CsvRowToListStringFn()))
              .setCoder(ListCoder.of(StringUtf8Coder.of()));

      PAssert.that(recordsAndSchema.get(recordsTag))
          .satisfies(
              new CheckFlatRecordColNamesFn(
                  ImmutableList.of("col_0", "col_1", "col_2", "col_3", "col_4")));
      PAssert.that(colValues).containsInAnyOrder(testCsvRecords);
      PAssert.thatSingleton(schema)
          .satisfies(
              new JsonAssertEqualsFn(
                  TestResourceLoader.classPath().loadAsString("five_column_csv_schema.json")));

      testPipeline.run().waitUntilFinish();
    }

    private static class CheckFlatRecordColNamesFn
        extends SimpleFunction<Iterable<FlatRecord>, Void> {

      private final ImmutableList<String> expectedFlatKeys;
      private final ImmutableList<String> expectedSchemaKeys;

      public CheckFlatRecordColNamesFn(ImmutableList<String> headers) {

        var expectedFlatKeysBuilder = ImmutableList.<String>builder();
        var expectedSchemaKeysBuilder = ImmutableList.<String>builder();

        for (String header : headers) {
          expectedFlatKeysBuilder.add("$." + header);
          expectedSchemaKeysBuilder.add("$.CsvRecord." + header);
        }

        this.expectedFlatKeys = expectedFlatKeysBuilder.build();
        this.expectedSchemaKeys = expectedSchemaKeysBuilder.build();
      }

      @Override
      public Void apply(Iterable<FlatRecord> input) {
        input.forEach(
            record -> {
              assertThat(record.getFlatKeySchemaMap().keySet())
                  .containsExactlyElementsIn(expectedFlatKeys);
              assertThat(record.getFlatKeySchemaMap().values())
                  .containsExactlyElementsIn(expectedSchemaKeys);
              assertThat(record.getValuesMap().keySet())
                  .containsExactlyElementsIn(expectedFlatKeys);
            });

        return null;
      }
    }

    private static class CsvRowToListStringFn extends SimpleFunction<CsvRow, List<String>> {

      @Override
      public List<String> apply(CsvRow input) {
        return ImmutableList.copyOf(input);
      }
    }
  }

  private static final class JsonAssertEqualsFn implements SerializableFunction<String, Void> {

    private final String expectedJson;

    public JsonAssertEqualsFn(String expectedJson) {
      this.expectedJson = expectedJson;
    }

    @Override
    public Void apply(String input) {
      try {
        JSONAssert.assertEquals(expectedJson, input, true);
      } catch (JSONException jsonException) {
        throw new AssertionFailedError(jsonException.getMessage());
      }
      return null;
    }
  }
}
