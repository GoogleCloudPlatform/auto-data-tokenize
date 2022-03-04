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

import static com.google.cloud.solutions.autotokenize.common.CsvRowFlatRecordConvertors.makeCsvAvroSchema;
import static com.google.cloud.solutions.autotokenize.testing.TestDbContainerFactory.makeTestMySQLContainer;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.common.DeIdentifiedRecordSchemaConverter;
import com.google.cloud.solutions.autotokenize.common.DeidentifyColumns;
import com.google.cloud.solutions.autotokenize.common.SecretsClient;
import com.google.cloud.solutions.autotokenize.dlp.DlpClientFactory;
import com.google.cloud.solutions.autotokenize.dlp.PartialBatchAccumulator;
import com.google.cloud.solutions.autotokenize.encryptors.FixedClearTextKeySetExtractor;
import com.google.cloud.solutions.autotokenize.testing.RecordsCountMatcher;
import com.google.cloud.solutions.autotokenize.testing.TestCsvFileGenerator;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.cloud.solutions.autotokenize.testing.stubs.dlp.Base64EncodingDlpStub;
import com.google.cloud.solutions.autotokenize.testing.stubs.dlp.StubbingDlpClientFactory;
import com.google.cloud.solutions.autotokenize.testing.stubs.kms.Base64DecodingKmsStub;
import com.google.cloud.solutions.autotokenize.testing.stubs.secretmanager.ConstantSecretVersionValueManagerServicesStub;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.truth.Truth;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.testcontainers.containers.JdbcDatabaseContainer;

@RunWith(Parameterized.class)
public final class EncryptionPipelineTest implements Serializable {

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  @Rule public transient TestPipeline readPipeline = TestPipeline.create();

  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String PROJECT_ID = "test-project";

  private final ImmutableMap<String, String> configParameters;
  private final SecretsClient secretsClient;
  private final String baseArgs;
  private final String inputSchemaJsonFile;
  private final int expectedRecordsCount;

  private JdbcDatabaseContainer<?> databaseContainer;
  private transient EncryptionPipelineOptions pipelineOptions;
  private transient DlpClientFactory dlpClientFactory;
  private transient Schema inputSchema;
  private transient Schema expectedSchema;

  @After
  public void tearDownTestDB() {
    if (databaseContainer != null) {
      databaseContainer.stop();
    }
  }

  @Test
  public void buildPipeline_valid() throws Exception {
    makeDlpStub();

    Truth.assertThat(
            Sets.difference(
                    Set.copyOf(expectedSchema.getFields()), Set.copyOf(inputSchema.getFields()))
                .size())
        .isAtLeast(1);

    // Perform the Encryption pipeline
    new EncryptionPipeline(
            pipelineOptions,
            testPipeline,
            dlpClientFactory,
            secretsClient,
            KeyManagementServiceClient.create(
                new Base64DecodingKmsStub(pipelineOptions.getMainKmsKeyUri())),
            new FixedClearTextKeySetExtractor(pipelineOptions.getTinkEncryptionKeySetJson()))
        .run()
        .waitUntilFinish();

    // Verify the output Avro file
    PCollection<GenericRecord> encryptedRecords =
        readPipeline.apply(
            AvroIO.readGenericRecords(expectedSchema)
                .from(pipelineOptions.getOutputDirectory() + "/*"));

    PAssert.that(encryptedRecords).satisfies(new RecordsCountMatcher<>(expectedRecordsCount));

    if (configParameters.containsKey("expectedRecordsAvro")) {
      var expectedRecords =
          TestResourceLoader.classPath()
              .forAvro()
              .readFile(configParameters.get("expectedRecordsAvro"))
              .loadAllRecords();

      PAssert.that(encryptedRecords).containsInAnyOrder(expectedRecords);
    }

    readPipeline.run();
  }

  public EncryptionPipelineTest(
      String testCondition,
      ImmutableMap<String, String> configParameters,
      String baseArgs,
      String inputSchemaJsonFile,
      int expectedRecordsCount) {
    this.configParameters = configParameters;
    this.baseArgs = baseArgs;
    this.secretsClient =
        SecretsClient.withSecretsStub(
            ConstantSecretVersionValueManagerServicesStub.of("resource/id/of/password/secret", ""));
    this.inputSchemaJsonFile = inputSchemaJsonFile;
    this.expectedRecordsCount = expectedRecordsCount;
  }

  @Parameters(name = "{0}")
  public static ImmutableList<Object[]> testParameters() {
    return ImmutableList.<Object[]>builder()
        .add(
            new Object[] {
              /*testCondition=*/ "Nested Repeated Record: 1 record (DLP)",
              /*configParameters=*/ ImmutableMap.of(
                  "testFile",
                  "avro_records/nested_repeated/record.avro",
                  "dlpEncryptConfigFile",
                  "avro_records/nested_repeated/encrypt_config.json"),
              /*baseArgs*/ "--sourceType=AVRO",
              /*inputSchemaJsonFile=*/ "avro_records/nested_repeated/schema.json",
              /*expectedRecordsCount=*/ 1
            })
        .add(
            new Object[] {
              /*testCondition=*/ "Use Regional DLP Endpoint",
              /*configParameters=*/ ImmutableMap.of(
                  "testFile",
                  "avro_records/nested_repeated/record.avro",
                  "dlpEncryptConfigFile",
                  "avro_records/nested_repeated/encrypt_config.json"),
              /*baseArgs*/ "--sourceType=AVRO --dlpRegion=us-central1",
              /*inputSchemaJsonFile=*/ "avro_records/nested_repeated/schema.json",
              /*expectedRecordsCount=*/ 1
            })
        .add(
            new Object[] {
              /*testCondition=*/ "Avro input: 1000 records (DLP)",
              /*configParameters=*/ ImmutableMap.of(
                  "testFile",
                  "userdata.avro",
                  "dlpEncryptConfigFile",
                  "email_cc_dlp_encrypt_config.json"),
              /*baseArgs*/ "--sourceType=AVRO",
              /*inputSchemaJsonFile=*/ "avro_records/userdata_records/schema.json",
              /*expectedRecordsCount=*/ 1000
            })
        .add(
            new Object[] {
              /*testCondition=*/ "Avro input: 1000 records (TINK)",
              /*configParameters=*/ ImmutableMap.of(
                  "testFile",
                  "userdata.avro",
                  "tinkEncryptionKeySetJsonFile",
                  "test_encryption_key.json"),
              /*baseArgs*/ "--sourceType=AVRO --tokenizeColumns=$.kylosample.cc"
                  + " --tokenizeColumns=$.kylosample.email",
              /*inputSchemaJsonFile=*/ "avro_records/userdata_records/schema.json",
              /*expectedRecordsCount=*/ 1000
            })
        .add(
            new Object[] {
              /*testCondition=*/ "CSV input: 1000 records with Provided Avro Schema (TINK)",
              /*configParameters=*/ ImmutableMap.of(
                  "tinkEncryptionKeySetJsonFile", "test_encryption_key.json"),
              /*baseArgs*/ "--sourceType=CSV_FILE --tokenizeColumns=$.CsvRecord.col_1",
              /*inputSchemaJsonFile=*/ "five_column_csv_schema.json",
              /*expectedRecordsCount=*/ 100
            })
        .add(
            new Object[] {
              /*testCondition=*/ "CSV input: 1000 records with Headers (TINK)",
              /*configParameters=*/ ImmutableMap.of(
                  "testFile",
                  "csv/sample-data-chats.csv",
                  "expectedRecordsAvro",
                  "csv/tink_encrypted_transcripts.avro",
                  "tinkEncryptionKeySetJsonFile",
                  "test_encryption_key.json"),
              /*baseArgs*/
              Joiner.on(' ')
                  .join(
                      "--sourceType=CSV_FILE",
                      "--tokenizeColumns=$.CsvRecord.transcript",
                      "--csvHeaders=chatId,userType,transcript,segmentId,segmentTimestamp"),
              /*inputSchemaJsonFile=*/ null,
              /*expectedRecordsCount=*/ 100
            })
        .add(
            new Object[] {
              /*testCondition=*/ "CSV input: 1000 records with Headers (DLP)",
              /*configParameters=*/ ImmutableMap.of(
                  "testFile", "csv/sample-data-chats.csv",
                  "expectedRecordsAvro", "csv/base64_encrypted_transcripts.avro",
                  "dlpEncryptConfigFile", "csv/transcript_dlp_encrypt_config.json"),
              /*baseArgs*/
              Joiner.on(' ')
                  .join(
                      "--sourceType=CSV_FILE",
                      "--csvHeaders=chatId,userType,transcript,segmentId,segmentTimestamp"),
              /*inputSchemaJsonFile=*/ null,
              /*expectedRecordsCount=*/ 100
            })
        .add(
            new Object[] {
              /*testCondition=*/ "JDBC input: 500 records [plainPassword]",
              /*configParameters=*/ ImmutableMap.of(
                  "initScript",
                  "db_init_scripts/contacts5k.sql",
                  "dlpEncryptConfigFile",
                  "contacts5k_dlp_encrypt_config.json"),
              /*baseArgs*/ "--sourceType=JDBC_TABLE "
                  + "--inputPattern=Contacts "
                  + "--jdbcDriverClass=com.mysql.cj.jdbc.Driver "
                  + "--jdbcFilterClause=ROUND(MOD(row_id, 10)) IN (1) "
                  + "--jdbcUserName=root "
                  + "--jdbcPassword=",
              /*inputSchemaJsonFile=*/ "Contacts5kSql_avro_schema.json",
              /*expectedRecordsCount=*/ 500
            })
        .add(
            new Object[] {
              /*testCondition=*/ "JDBC input: 500 records [passwordSecret]",
              /*configParameters=*/ ImmutableMap.of(
                  "initScript",
                  "db_init_scripts/contacts5k.sql",
                  "dlpEncryptConfigFile",
                  "contacts5k_dlp_encrypt_config.json"),
              /*baseArgs*/ "--sourceType=JDBC_TABLE "
                  + "--inputPattern=Contacts "
                  + "--jdbcDriverClass=com.mysql.cj.jdbc.Driver "
                  + "--jdbcFilterClause=ROUND(MOD(row_id, 10)) IN (1) "
                  + "--jdbcUserName=root "
                  + "--jdbcPasswordSecretsKey=resource/id/of/password/secret",
              /*inputSchemaJsonFile=*/ "Contacts5kSql_avro_schema.json",
              /*expectedRecordsCount=*/ 500
            })
        .add(
            new Object[] {
              /*testCondition=*/ "Custom_Value_Tokenizer_with_KMS_Key",
              /*configParameters=*/ ImmutableMap.of("testFile", "userdata.avro"),
              /*baseArgs*/ "--sourceType=AVRO --tokenizeColumns=$.kylosample.cc"
                  + " --tokenizeColumns=$.kylosample.email"
                  + " --mainKmsKeyUri=projects/test-projects/global/keysets/test-keyset/keys/test-key1"
                  + " --keyMaterial=Y21oeWFYWnFkVzk0YTJSb2VIQmxaMmR1YUhkelluWmxlR1pvZFhoNmNYRT0="
                  + " --keyMaterialType=GCP_KMS_WRAPPED_KEY"
                  + " --valueTokenizerFactoryFullClassName=com.google.cloud.solutions.autotokenize.encryptors.AesEcbStringValueTokenizer$AesEcbValueTokenizerFactory",
              /*inputSchemaJsonFile=*/ "avro_records/userdata_records/schema.json",
              /*expectedRecordsCount=*/ 1000
            })
        .add(
            new Object[] {
              /*testCondition=*/ "Custom_Value_Tokenizer_with_Raw_Base64_Key",
              /*configParameters=*/ ImmutableMap.of("testFile", "userdata.avro"),
              /*baseArgs*/ "--sourceType=AVRO --tokenizeColumns=$.kylosample.cc"
                  + " --tokenizeColumns=$.kylosample.email"
                  + " --keyMaterial=cmhyaXZqdW94a2RoeHBlZ2duaHdzYnZleGZodXh6cXE="
                  + " --keyMaterialType=RAW_BASE64_KEY"
                  + " --valueTokenizerFactoryFullClassName=com.google.cloud.solutions.autotokenize.encryptors.AesEcbStringValueTokenizer$AesEcbValueTokenizerFactory",
              /*inputSchemaJsonFile=*/ "avro_records/userdata_records/schema.json",
              /*expectedRecordsCount=*/ 1000
            })
        .add(
            new Object[] {
              /*testCondition=*/ "Custom_Value_Tokenizer_with_Raw_UTF8_Key",
              /*configParameters=*/ ImmutableMap.of("testFile", "userdata.avro"),
              /*baseArgs*/ "--sourceType=AVRO --tokenizeColumns=$.kylosample.cc"
                  + " --tokenizeColumns=$.kylosample.email"
                  + " --keyMaterial=rhrivjuoxkdhxpeggnhwsbvexfhuxzqq"
                  + " --keyMaterialType=RAW_UTF8_KEY"
                  + " --valueTokenizerFactoryFullClassName=com.google.cloud.solutions.autotokenize.encryptors.AesEcbStringValueTokenizer$AesEcbValueTokenizerFactory",
              /*inputSchemaJsonFile=*/ "avro_records/userdata_records/schema.json",
              /*expectedRecordsCount=*/ 1000
            })
        .build();
  }

  @Before
  @SuppressWarnings("UnstableApiUsage")
  public void makeOptions() throws Exception {
    var options =
        Splitter.on("--")
            .splitToStream(baseArgs)
            .filter(StringUtils::isNotBlank)
            .map(String::trim)
            .map(opt -> Splitter.on('=').splitToList(opt))
            .map(x -> Map.entry(x.get(0), x.get(1)))
            .collect(Collectors.groupingBy(Map.Entry::getKey))
            .entrySet()
            .stream()
            .map(
                e ->
                    Map.entry(
                        e.getKey(),
                        e.getValue().stream().map(Map.Entry::getValue).collect(toList())))
            .map(
                e ->
                    Map.entry(
                        e.getKey(),
                        (e.getValue().size() == 1) ? e.getValue().get(0) : e.getValue()))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

    var sourceType = SourceType.valueOf((String) options.get("sourceType"));
    inputSchema =
        (inputSchemaJsonFile != null)
            ? TestResourceLoader.classPath().forAvro().asSchema(inputSchemaJsonFile)
            : ((options.get("csvHeaders") != null)
                ? makeCsvAvroSchema(
                    Splitter.on(',').splitToList((String) options.get("csvHeaders")))
                : null);

    if (configParameters.containsKey("dlpEncryptConfigFile")) {
      options.put(
          "dlpEncryptConfigJson",
          TestResourceLoader.classPath()
              .loadAsString(configParameters.get("dlpEncryptConfigFile")));
    }

    if (configParameters.containsKey("tinkEncryptionKeySetJsonFile")) {
      options.put(
          "tinkEncryptionKeySetJson",
          TestResourceLoader.classPath()
              .loadAsString(configParameters.get("tinkEncryptionKeySetJsonFile")));
      options.put("mainKmsKeyUri", "project/my-project/locations");
    }

    if (!configParameters.containsKey("dlpEncryptConfigFile")) {
      var tokenizeColumnsOption = options.get("tokenizeColumns");
      //noinspection unchecked testing class.
      var encryptColumns =
          List.class.isAssignableFrom(tokenizeColumnsOption.getClass())
              ? (List<String>) tokenizeColumnsOption
              : ImmutableList.of((String) tokenizeColumnsOption);
      expectedSchema = makeExpectedSchema(inputSchema.toString(), encryptColumns);
    }

    switch (sourceType) {
      case PARQUET:
      case AVRO:
        var testAvroFileFolder = temporaryFolder.newFolder();
        TestResourceLoader.classPath()
            .copyTo(testAvroFileFolder)
            .createFileTestCopy(configParameters.get("testFile"));
        options.put("inputPattern", testAvroFileFolder.getAbsolutePath() + "/*");
        break;

      case CSV_FILE:
        String testCsvFile = temporaryFolder.newFolder().getAbsolutePath() + "/random.csv";

        if (configParameters.containsKey("testFile")) {
          testCsvFile =
              TestResourceLoader.classPath()
                  .copyTo(temporaryFolder.newFolder())
                  .createFileTestCopy(configParameters.get("testFile"))
                  .getAbsolutePath();
        } else {
          TestCsvFileGenerator.create()
              .withRowCount(expectedRecordsCount)
              .writeRandomRecordsToFile(testCsvFile);
        }
        options.put("inputPattern", testCsvFile);
        break;

      case JDBC_TABLE:
        databaseContainer = makeTestMySQLContainer(configParameters.get("initScript"));
        databaseContainer.start();
        // update connection url:
        options.put("jdbcConnectionUrl", databaseContainer.getJdbcUrl());
        break;
      case BIGQUERY_TABLE:
      case BIGQUERY_QUERY:
      case UNRECOGNIZED:
      case UNKNOWN_FILE_TYPE:
        throw new IllegalArgumentException("Unsupported Test Type");
    }

    if (inputSchemaJsonFile != null) {
      options.put("schema", inputSchema.toString());
    }

    options.put("outputDirectory", temporaryFolder.newFolder().getAbsolutePath());
    options.put("project", PROJECT_ID);

    var completeArgs =
        options.entrySet().stream()
            .flatMap(flattenRepeatedValues())
            .map(e -> String.format("--%s=%s", e.getKey(), e.getValue()))
            .toArray(String[]::new);

    pipelineOptions =
        PipelineOptionsFactory.fromArgs(completeArgs).as(EncryptionPipelineOptions.class);
  }

  private Function<Entry<String, ?>, Stream<? extends Entry<String, ?>>> flattenRepeatedValues() {
    return entry ->
        (entry.getValue() instanceof List)
            ? ((List<?>) entry.getValue())
                .stream().map(repeatedValue -> Map.entry(entry.getKey(), repeatedValue))
            : Stream.of(entry);
  }

  public void makeDlpStub() {
    if (pipelineOptions.getDlpEncryptConfigJson() == null) {
      return;
    }

    var dlpEncryptConfig =
        TestResourceLoader.classPath()
            .forProto(DlpEncryptConfig.class)
            .loadJson(configParameters.get("dlpEncryptConfigFile"));

    var encryptSchemaColumns = DeidentifyColumns.columnNamesIn(dlpEncryptConfig);

    expectedSchema = makeExpectedSchema(inputSchema.toString(), encryptSchemaColumns);

    var encryptColumns =
        encryptSchemaColumns.stream()
            .map(
                name ->
                    Pattern.compile("^(\\$\\.)?([^\\.]+\\.)(.*)$").matcher(name).replaceAll("$1$3"))
            .collect(toImmutableList());

    dlpClientFactory =
        new StubbingDlpClientFactory(
            new Base64EncodingDlpStub(
                PartialBatchAccumulator.RECORD_ID_COLUMN_NAME,
                encryptColumns,
                PROJECT_ID,
                pipelineOptions.getDlpRegion()));
  }

  private static Schema makeExpectedSchema(
      String inputSchemaJson, List<String> encryptSchemaColumns) {
    return DeIdentifiedRecordSchemaConverter.withOriginalSchemaJson(inputSchemaJson)
        .withEncryptColumnKeys(encryptSchemaColumns)
        .updatedSchema();
  }
}
