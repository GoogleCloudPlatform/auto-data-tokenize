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
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

public final class TransformingReaderTest {

  @RunWith(Parameterized.class)
  public static final class JdbcReaderTest {

    @Rule public transient TestPipeline testPipeline = TestPipeline.create();

    private JdbcDatabaseContainer<?> databaseContainer;

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
      this.testDatabaseName = "test_" + UUID.randomUUID().toString().replaceAll("[\\-]", "_");
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

    private String testFileFolder;

    @Parameters(name = "{0} file contains {1} records")
    public static Collection<Object[]> differentFileTypes() {
      return ImmutableList.<Object[]>builder()
          .add(new Object[] {SourceType.AVRO, 1000})
          .add(new Object[] {SourceType.PARQUET, 1000})
          .build();
    }

    public AvroParquetReaderTest(SourceType fileSourceType, int expectedRecordsCount) {
      this.fileSourceType = fileSourceType;
      this.expectedRecordsCount = expectedRecordsCount;
    }

    @Before
    public void generateTestRecordsFile() throws IOException {
      Schema testSchema = RandomGenericRecordGenerator.SCHEMA;
      File tempFolder = temporaryFolder.newFolder();
      if (tempFolder.mkdirs()) {
        throw new IOException("unable to create temporary directories for testing.");
      }

      testFileFolder = tempFolder.getAbsolutePath();

      logger.atInfo().log("Writing records to: %s", testFileFolder);

      writePipeline
          .apply(
              Create.of(generateGenericRecords(expectedRecordsCount))
                  .withCoder(AvroCoder.of(testSchema)))
          .apply(
              FileIO.<GenericRecord>write()
                  .via(fileTypeSpecificSink(testSchema))
                  .to(testFileFolder));

      writePipeline.run().waitUntilFinish();
    }

    @Test
    public void expand_providedFile_recordCountMatched() {
      var recordsTag = new TupleTag<FlatRecord>();
      var schemaTag = new TupleTag<String>();

      var recordsAndSchemaTuple =
          readPipeline.apply(
              TransformingReader.forSourceType(fileSourceType)
                  .from(testFileFolder + "/*")
                  .withRecordsTag(recordsTag)
                  .withAvroSchemaTag(schemaTag));

      var records = recordsAndSchemaTuple.get(recordsTag);
      var schema = recordsAndSchemaTuple.get(schemaTag);

      PAssert.that(records).satisfies(hasRecordCount(expectedRecordsCount));
      PAssert.that(schema).satisfies(hasRecordCount(1));

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
      JSONAssert.assertEquals(expectedJson, input, true);
      return null;
    }
  }
}
