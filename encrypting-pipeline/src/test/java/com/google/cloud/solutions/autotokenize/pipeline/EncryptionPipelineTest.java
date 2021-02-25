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

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.common.DeIdentifiedRecordSchemaConverter;
import com.google.cloud.solutions.autotokenize.common.DeidentifyColumns;
import com.google.cloud.solutions.autotokenize.common.RecordFlattener;
import com.google.cloud.solutions.autotokenize.common.util.JsonConvertor;
import com.google.cloud.solutions.autotokenize.pipeline.dlp.DlpClientFactory;
import com.google.cloud.solutions.autotokenize.pipeline.dlp.PartialColumnBatchAccumulator;
import com.google.cloud.solutions.autotokenize.testing.MatchRecordsCountFn;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.cloud.solutions.autotokenize.testing.stubs.dlp.Base64EncodingDlpStub;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class EncryptionPipelineTest implements Serializable {

  @Rule public transient TestPipeline mainPipeline = TestPipeline.create();

  @Rule public transient TestPipeline readPipeline = TestPipeline.create();

  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  private transient String tempAvroFile;

  private static final ImmutableList<GenericRecord> TEST_RECORDS;
  private static final Schema TEST_RECORD_SCHEMA;
  private static final String TEST_DLP_ENCRYPT_CONFIG_JSON;
  private static final String PROJECT_ID = "test-project";

  static {
    TEST_RECORDS =
        TestResourceLoader.classPath().forAvro().readFile("userdata.avro").loadAllRecords();
    TEST_RECORD_SCHEMA = TEST_RECORDS.get(0).getSchema();
    TEST_DLP_ENCRYPT_CONFIG_JSON =
        TestResourceLoader.classPath().loadAsString("email_cc_dlp_encrypt_config.json");
  }

  @Before
  public void makeTempCopyOfUserDataAvro() throws IOException {
    tempAvroFile =
        TestResourceLoader.classPath()
            .copyTo(temporaryFolder.newFolder())
            .createFileTestCopy("userdata.avro")
            .getAbsolutePath();
  }

  @Test
  public void expand() throws Exception {
    EncryptingPipelineOptions options = PipelineOptionsFactory.as(EncryptingPipelineOptions.class);
    options.setDlpEncryptConfigJson(TEST_DLP_ENCRYPT_CONFIG_JSON);
    options.setInputPattern(tempAvroFile);
    options.setSourceType(SourceType.AVRO);
    options.setSchema(TEST_RECORD_SCHEMA.toString());
    options.setOutputDirectory(temporaryFolder.newFolder().getAbsolutePath());
    options.setTokenizeColumns(
        DeidentifyColumns.columnNamesIn(
            JsonConvertor.parseJson(TEST_DLP_ENCRYPT_CONFIG_JSON, DlpEncryptConfig.class)));
    options.setProject(PROJECT_ID);
    Schema encryptedSchema =
        DeIdentifiedRecordSchemaConverter.withOriginalSchema(TEST_RECORD_SCHEMA)
            .withEncryptColumnKeys(options.getTokenizeColumns())
            .updatedSchema();

    // Perform the Encryption pipeline
    new EncryptionPipeline.EncryptingPipelineFactory(
            options, mainPipeline, DlpClientFactory.withStub(makeDlpStub()))
        .buildPipeline()
        .run()
        .waitUntilFinish();

    // Read the output Avro file
    PCollection<GenericRecord> encryptedRecords =
        readPipeline.apply(
            AvroIO.readGenericRecords(encryptedSchema).from(options.getOutputDirectory() + "/*"));

    PAssert.that(encryptedRecords).satisfies(new MatchRecordsCountFn<>(TEST_RECORDS.size()));

    readPipeline.run();
  }

  private static Base64EncodingDlpStub makeDlpStub() {
    return new Base64EncodingDlpStub(
        PartialColumnBatchAccumulator.RECORD_ID_COLUMN_NAME, buildExpectedHeaders(), PROJECT_ID);
  }

  private static ImmutableList<String> buildExpectedHeaders() {
    ImmutableList<String> encryptColumns =
        DeidentifyColumns.columnNamesIn(
            JsonConvertor.parseJson(TEST_DLP_ENCRYPT_CONFIG_JSON, DlpEncryptConfig.class));

    return TEST_RECORDS.stream()
        .map(RecordFlattener.forGenericRecord()::flatten)
        .map(FlatRecord::getFlatKeySchemaMap)
        .map(Map::entrySet)
        .flatMap(Set::stream)
        .filter(e -> encryptColumns.contains(e.getValue()))
        .map(Map.Entry::getKey)
        .distinct()
        .collect(ImmutableList.toImmutableList());
  }
}
