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

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.JdbcConfiguration;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.testing.RandomGenericRecordGenerator;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.cloud.solutions.autotokenize.testing.stubs.secretmanager.ConstantSecretVersionValueManagerServicesStub;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.testcontainers.containers.MySQLContainer;

public final class TransformingReaderTest {

  @RunWith(Parameterized.class)
  public static final class JdbcReaderTest {

    @Rule public transient TestPipeline testPipeline = TestPipeline.create();

    private static MySQLContainer databaseContainer;

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

      databaseContainer = new MySQLContainer("mysql");
      databaseContainer.withDatabaseName(testDatabaseName);
      databaseContainer.withUsername("root");
      databaseContainer.withPassword("");
      databaseContainer.withInitScript(initScriptFile);
      databaseContainer.start();
    }

    @AfterClass
    public static void tearDownDatabase() throws Exception {
      databaseContainer.stop();
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
}
