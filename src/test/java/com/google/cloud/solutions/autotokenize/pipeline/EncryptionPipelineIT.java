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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.common.DeIdentifiedRecordSchemaConverter;
import com.google.cloud.solutions.autotokenize.common.DeidentifyColumns;
import com.google.cloud.solutions.autotokenize.common.SecretsClient;
import com.google.cloud.solutions.autotokenize.dlp.DlpClientFactory;
import com.google.cloud.solutions.autotokenize.dlp.PartialBatchAccumulator;
import com.google.cloud.solutions.autotokenize.testing.RecordsCountMatcher;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.cloud.solutions.autotokenize.testing.stubs.dlp.Base64EncodingDlpStub;
import com.google.cloud.solutions.autotokenize.testing.stubs.dlp.StubbingDlpClientFactory;
import com.google.cloud.solutions.autotokenize.testing.stubs.secretmanager.ConstantSecretVersionValueManagerServicesStub;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.JsonKeysetWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class EncryptionPipelineIT implements Serializable {

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  @Rule public transient TestPipeline readPipeline = TestPipeline.create();

  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String PROJECT_ID = "test-project";

  private final String testCondition;
  private final ImmutableMap<String, String> configParameters;
  private final SecretsClient secretsClient;
  private final String baseArgs;
  private final String inputSchemaJsonFile;
  private final int expectedRecordsCount;

  public static transient DB testDBInstance;
  private transient EncryptionPipelineOptions pipelineOptions;
  private transient DlpClientFactory dlpClientFactory;
  private transient Schema expectedSchema;
  private transient String clearTextEncryptionKeySet;

  @BeforeClass
  public static void setupMariaDBInMemory() throws ManagedProcessException {
    testDBInstance = DB.newEmbeddedDB(0);
    testDBInstance.start();
  }

  @AfterClass
  public static void tearDownTestDB() throws ManagedProcessException {
    if (testDBInstance != null) {
      testDBInstance.stop();
    }
  }

  @Before
  public void setupClearTextKeySetHandle() throws GeneralSecurityException, IOException {
    var tinkKeySet = pipelineOptions.getTinkEncryptionKeySetJson();

    if (tinkKeySet == null) {
      clearTextEncryptionKeySet = null;
      return;
    }

    var baos = new ByteArrayOutputStream();
    CleartextKeysetHandle.write(
        CleartextKeysetHandle.read(JsonKeysetReader.withString(tinkKeySet)),
        JsonKeysetWriter.withOutputStream(baos));
    clearTextEncryptionKeySet = baos.toString();
  }

  @Test
  public void buildPipeline_valid() throws Exception {

    // Perform the Encryption pipeline
    new EncryptionPipeline.EncryptingPipelineFactory(
            pipelineOptions,
            testPipeline,
            dlpClientFactory,
            secretsClient,
            clearTextEncryptionKeySet)
        .buildPipeline()
        .run()
        .waitUntilFinish();

    // Verify the output Avro file
    PCollection<GenericRecord> encryptedRecords =
        readPipeline.apply(
            AvroIO.readGenericRecords(expectedSchema)
                .from(pipelineOptions.getOutputDirectory() + "/*"));

    PAssert.that(encryptedRecords).satisfies(new RecordsCountMatcher<>(expectedRecordsCount));

    readPipeline.run();
  }

  public EncryptionPipelineIT(
      String testCondition,
      ImmutableMap<String, String> configParameters,
      String baseArgs,
      String inputSchemaJsonFile,
      int expectedRecordsCount) {
    this.testCondition = testCondition;
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
              /*baseArgs*/ "--sourceType=AVRO --tokenizeColumns=$.kylosample.cc --tokenizeColumns=$.kylosample.email",
              /*inputSchemaJsonFile=*/ "avro_records/userdata_records/schema.json",
              /*expectedRecordsCount=*/ 1000
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
        .build();
  }

  @Before
  @SuppressWarnings("UnstableApiUsage")
  public void makeOptions() throws Exception {
    var options =
        Splitter.on(CharMatcher.anyOf("--"))
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
    var inputSchemaJson = TestResourceLoader.classPath().loadAsString(inputSchemaJsonFile);

    if (configParameters.containsKey("dlpEncryptConfigFile")) {
      options.put(
          "dlpEncryptConfigJson",
          TestResourceLoader.classPath()
              .loadAsString(configParameters.get("dlpEncryptConfigFile")));
      makeDlpStub();
    }

    if (configParameters.containsKey("tinkEncryptionKeySetJsonFile")) {
      options.put(
          "tinkEncryptionKeySetJson",
          TestResourceLoader.classPath()
              .loadAsString(configParameters.get("tinkEncryptionKeySetJsonFile")));
      options.put("mainKmsKeyUri", "project/my-project/locations");

      //noinspection unchecked testing class.
      expectedSchema =
          DeIdentifiedRecordSchemaConverter.withOriginalSchemaJson(inputSchemaJson)
              .withEncryptColumnKeys((List<String>) options.get("tokenizeColumns"))
              .updatedSchema();
    }

    switch (sourceType) {
      case PARQUET:
      case AVRO:
        var testAvroFileFolder = temporaryFolder.newFolder();
        TestResourceLoader.absolutePath()
            .copyTo(testAvroFileFolder)
            .createFileTestCopy(configParameters.get("testFile"));
        options.put("inputPattern", testAvroFileFolder.getAbsolutePath() + "/*");
        break;

      case JDBC_TABLE:
        var initScript = configParameters.get("initScript");
        var testDatabaseName = "test_" + new Random().nextLong();
        testDBInstance.createDB(testDatabaseName);
        testDBInstance.source(initScript, testDatabaseName);
        // update connection url:
        options.put(
            "jdbcConnectionUrl", testDBInstance.getConfiguration().getURL(testDatabaseName));
        break;
      case BIGQUERY_TABLE:
      case BIGQUERY_QUERY:
      case UNRECOGNIZED:
      case UNKNOWN_FILE_TYPE:
        throw new IllegalArgumentException("Unsupported Test Type");
    }

    options.put("schema", inputSchemaJson);
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

  private Function<Entry<String, Object>, Stream<? extends Entry<String, ? extends Object>>>
      flattenRepeatedValues() {
    return entry ->
        (entry.getValue() instanceof List)
            ? ((List) entry.getValue())
                .stream().map(repeatedValue -> Map.entry(entry.getKey(), repeatedValue))
            : Stream.of(entry);
  }

  public void makeDlpStub() {
    var dlpEncryptConfig =
        TestResourceLoader.classPath()
            .forProto(DlpEncryptConfig.class)
            .loadJson(configParameters.get("dlpEncryptConfigFile"));

    var encryptSchemaColumns = DeidentifyColumns.columnNamesIn(dlpEncryptConfig);

    expectedSchema =
        DeIdentifiedRecordSchemaConverter.withOriginalSchemaJson(
                TestResourceLoader.classPath().loadAsString(inputSchemaJsonFile))
            .withEncryptColumnKeys(encryptSchemaColumns)
            .updatedSchema();

    var encryptColumns =
        encryptSchemaColumns.stream()
            .map(
                name ->
                    Pattern.compile("^(\\$\\.)?([^\\.]+\\.)(.*)$").matcher(name).replaceAll("$1$3"))
            .collect(toImmutableList());

    dlpClientFactory =
        new StubbingDlpClientFactory(
            new Base64EncodingDlpStub(
                PartialBatchAccumulator.RECORD_ID_COLUMN_NAME, encryptColumns, PROJECT_ID));
  }
}
