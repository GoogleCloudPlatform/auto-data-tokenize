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

package com.google.cloud.solutions.autotokenize.pipeline;

import static com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType.AVRO;
import static com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType.PARQUET;
import static com.google.cloud.solutions.autotokenize.testing.RandomGenericRecordGenerator.generateGenericRecords;
import static java.lang.Integer.parseInt;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.InfoTypeInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.common.util.JsonConvertor;
import com.google.cloud.solutions.autotokenize.testing.FileStringReader;
import com.google.cloud.solutions.autotokenize.testing.JsonSubject;
import com.google.cloud.solutions.autotokenize.testing.RandomGenericRecordGenerator;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.cloud.solutions.autotokenize.testing.stubs.dlp.ItemShapeValidatingDlpStub;
import com.google.cloud.solutions.autotokenize.testing.stubs.dlp.StubbingDlpClientFactory;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class DlpSamplerIdentifyPipelineIT {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final String TEST_DB_NAME = "test_contacts_db";

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();
  @Rule public transient TestPipeline sampleRecordWritePipeline = TestPipeline.create();
  @Rule public transient TestPipeline columnInfoReadingPipeline = TestPipeline.create();

  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final ImmutableMap<String, String> configParameters;
  private final String basicArgs;
  private final String expectedSchema;
  private final ImmutableList<ColumnInformation> expectedColumnInformation;
  private final ImmutableMap<String, String> schemaKeyInfoTypeMap;

  private transient String outputFolder;
  public transient DB testDatabase;
  public transient DlpSamplerIdentifyOptions pipelineOptions;

  @Test
  public void makePipeline_valid() throws IOException {
    var dlpStub =
        new ItemShapeValidatingDlpStub(pipelineOptions.getProject(), schemaKeyInfoTypeMap);

    new DlpSamplerIdentifyPipeline(
            pipelineOptions, testPipeline, new StubbingDlpClientFactory(dlpStub))
        .makePipeline()
        .run()
        .waitUntilFinish();

    var actualSchema =
        TestResourceLoader.absolutePath().loadAsString(outputFolder + "/schema.json");

    JsonSubject.assertThat(actualSchema).isEqualTo(expectedSchema);
    assertExpectedColumnInformation(pipelineOptions.getReportLocation());
  }

  private void assertExpectedColumnInformation(String reportLocation) {
    var outputColumnInfos =
        columnInfoReadingPipeline
            .apply(FileStringReader.create(reportLocation + "/col-*"))
            .apply(
                MapElements.into(TypeDescriptor.of(ColumnInformation.class))
                    .via(
                        (SerializableFunction<String, ColumnInformation>)
                            input -> JsonConvertor.parseJson(input, ColumnInformation.class)));

    PAssert.that(outputColumnInfos).containsInAnyOrder(expectedColumnInformation);
    columnInfoReadingPipeline.run().waitUntilFinish();
  }

  @Parameters(name = "{0}")
  public static ImmutableList<Object[]> testParameters() {
    return ImmutableList.<Object[]>builder()
        .add(
            new Object[] {
              "Avro File 10000 records",
              ImmutableMap.of("recordCount", "10000"),
              "--sourceType=AVRO",
              RandomGenericRecordGenerator.SCHEMA_STRING,
              ImmutableList.of(
                  ColumnInformation.newBuilder()
                      .setColumnName("$.testrecord.name")
                      .addInfoTypes(
                          InfoTypeInformation.newBuilder()
                              .setCount(1000)
                              .setInfoType("PERSON_NAME"))
                      .build()),
              ImmutableMap.of("$.testrecord.name", "PERSON_NAME")
            })
        .add(
            new Object[] {
              "Avro File 10000 Sampling 3000 records",
              ImmutableMap.of("recordCount", "10000"),
              "--sourceType=AVRO --sampleSize=3000",
              RandomGenericRecordGenerator.SCHEMA_STRING,
              ImmutableList.of(
                  ColumnInformation.newBuilder()
                      .setColumnName("$.testrecord.name")
                      .addInfoTypes(
                          InfoTypeInformation.newBuilder()
                              .setCount(3000)
                              .setInfoType("PERSON_NAME"))
                      .build()),
              ImmutableMap.of("$.testrecord.name", "PERSON_NAME")
            })
        .add(
            new Object[] {
              "Avro File 2000 Sampling 3000 records",
              ImmutableMap.of("recordCount", "2000"),
              "--sourceType=AVRO --sampleSize=3000",
              RandomGenericRecordGenerator.SCHEMA_STRING,
              ImmutableList.of(
                  ColumnInformation.newBuilder()
                      .setColumnName("$.testrecord.name")
                      .addInfoTypes(
                          InfoTypeInformation.newBuilder()
                              .setCount(2000)
                              .setInfoType("PERSON_NAME"))
                      .build()),
              ImmutableMap.of("$.testrecord.name", "PERSON_NAME")
            })
        .add(
            new Object[] {
              "Parquet File 10000 records",
              ImmutableMap.of("recordCount", "10000"),
              "--sourceType=PARQUET",
              RandomGenericRecordGenerator.SCHEMA_STRING,
              ImmutableList.of(
                  ColumnInformation.newBuilder()
                      .setColumnName("$.testrecord.name")
                      .addInfoTypes(
                          InfoTypeInformation.newBuilder()
                              .setCount(1000)
                              .setInfoType("PERSON_NAME"))
                      .build()),
              ImmutableMap.of("$.testrecord.name", "PERSON_NAME")
            })
        .add(
            new Object[] {
              "MySQL 5000 records",
              ImmutableMap.of("initScript", "db_init_scripts/contacts5k.sql"),
              "--sourceType=JDBC_TABLE --inputPattern=Contacts --jdbcDriverClass=com.mysql.cj.jdbc.Driver",
              TestResourceLoader.classPath().loadAsString("Contacts5kSql_avro_schema.json"),
              ImmutableList.of(
                  ColumnInformation.newBuilder()
                      .setColumnName("$.topLevelRecord.person_name")
                      .addInfoTypes(
                          InfoTypeInformation.newBuilder()
                              .setInfoType("PERSON_NAME")
                              .setCount(1000))
                      .build(),
                  ColumnInformation.newBuilder()
                      .setColumnName("$.topLevelRecord.contact_number")
                      .addInfoTypes(
                          InfoTypeInformation.newBuilder()
                              .setInfoType("PHONE_NUMBER")
                              .setCount(1000))
                      .build()),
              ImmutableMap.of(
                  "$.topLevelRecord.person_name", "PERSON_NAME",
                  "$.topLevelRecord.contact_number", "PHONE_NUMBER")
            })
        .build();
  }

  public DlpSamplerIdentifyPipelineIT(
      String testName,
      ImmutableMap<String, String> configParameters,
      String basicArgs,
      String expectedSchema,
      ImmutableList<ColumnInformation> expectedColumnInformation,
      ImmutableMap<String, String> schemaKeyInfoTypeMap) {
    this.configParameters = configParameters;
    this.basicArgs = basicArgs;
    this.expectedSchema = expectedSchema;
    this.expectedColumnInformation = expectedColumnInformation;
    this.schemaKeyInfoTypeMap = schemaKeyInfoTypeMap;
  }

  @Before
  public void createOutputFolder() throws IOException {
    outputFolder = temporaryFolder.newFolder().getAbsolutePath();
  }

  @Before
  @SuppressWarnings("UnstableApiUsage")
  public void makeOptions() throws IOException, ManagedProcessException {
    Map<String, String> options =
        Splitter.on(CharMatcher.anyOf("\n "))
            .splitToStream(basicArgs)
            .filter(StringUtils::isNotBlank)
            .map(opt -> Splitter.on('=').splitToList(opt))
            .map(x -> Map.entry(x.get(0), x.get(1)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    var sourceType = SourceType.valueOf(options.get("--sourceType"));

    switch (sourceType) {
      case AVRO:
        // Create Avro files
        var testAvroFileFolder = temporaryFolder.newFolder().getAbsolutePath();
        generateTestRecordsFile(AVRO, testAvroFileFolder);
        options.put("--inputPattern", testAvroFileFolder + "/*");
        break;

      case PARQUET:
        // Create Parquet files
        var testParquetFileFolder = temporaryFolder.newFolder().getAbsolutePath();
        generateTestRecordsFile(PARQUET, testParquetFileFolder);
        options.put("--inputPattern", testParquetFileFolder + "/*");
        break;
      case JDBC_TABLE:
        setupMariaDBInMemory();
        // update connection url:
        options.put(
            "--jdbcConnectionUrl",
            String.format(
                "%s?user=%s&password=%s",
                testDatabase.getConfiguration().getURL(TEST_DB_NAME), "root", ""));
        break;
      case BIGQUERY_TABLE:
      case BIGQUERY_QUERY:
      case UNRECOGNIZED:
      case UNKNOWN_FILE_TYPE:
        throw new IllegalArgumentException("Unsupported Test Type");
    }

    options.put("--project", "test-project");
    options.put("--reportLocation", outputFolder);

    var completeArgs =
        options.entrySet().stream()
            .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
            .toArray(String[]::new);

    pipelineOptions =
        PipelineOptionsFactory.fromArgs(completeArgs).as(DlpSamplerIdentifyOptions.class);
  }

  private void generateTestRecordsFile(SourceType sourceType, String folder) {

    int recordCount = parseInt(configParameters.getOrDefault("recordCount", "0"));

    Schema testSchema = RandomGenericRecordGenerator.SCHEMA;
    logger.atInfo().log("Writing records to: %s", folder);
    sampleRecordWritePipeline
        .apply(Create.of(generateGenericRecords(recordCount)).withCoder(AvroCoder.of(testSchema)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(
                    (sourceType.equals(AVRO)
                        ? AvroIO.sink(testSchema)
                        : ParquetIO.sink(testSchema)))
                .to(folder));

    sampleRecordWritePipeline.run().waitUntilFinish();
  }

  public void setupMariaDBInMemory() throws ManagedProcessException {
    var initScript = configParameters.get("initScript");
    testDatabase = DB.newEmbeddedDB(0);
    testDatabase.start();
    testDatabase.createDB(TEST_DB_NAME);
    testDatabase.source(initScript, TEST_DB_NAME);
  }

  @After
  public void tearDownTestDB() throws ManagedProcessException {
    if (testDatabase != null) {
      testDatabase.stop();
    }
  }
}
