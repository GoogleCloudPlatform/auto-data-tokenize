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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.common.DeIdentifiedRecordSchemaConverter;
import com.google.cloud.solutions.autotokenize.common.SourceNames;
import com.google.cloud.solutions.autotokenize.common.TransformingFileReader;
import com.google.cloud.solutions.autotokenize.pipeline.encryptors.DaeadEncryptingValueTokenizerFactory;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Creates and launches a Dataflow pipeline to encrypt the provided input files using the
 * column-names from the FileDlpReport.
 */
public class EncryptionPipeline {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static void main(String[] args) throws GeneralSecurityException, IOException {

    EncryptingPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).as(EncryptingPipelineOptions.class);

    if (options.getTokenizeColumns() == null || options.getTokenizeColumns().isEmpty()) {
      logger.atInfo().log(
        "====================%n" +
          "Abandoning Pipeline.%n" +
          "====================%n" +
          "No columns to tokenize%n" +
          "====================");
      return;
    }

    DataflowPipelineJob encryptingJob = (DataflowPipelineJob) EncryptingPipelineFactory
      .using(options).buildPipeline().run();

    logger.atInfo().log(
      "JobLink: https://console.cloud.google.com/dataflow/jobs/%s?project=%s",
      encryptingJob.getJobId(),
      encryptingJob.getProjectId());
  }

  /**
   * The pipeline creator.
   */
  private static class EncryptingPipelineFactory {

    private final EncryptingPipelineOptions options;
    private final Schema inputSchema;
    private final Schema encryptedSchema;
    private final Pipeline pipeline;
    private final String tinkKeySetJson;
    private final String mainKeyKmsUri;

    private EncryptingPipelineFactory(EncryptingPipelineOptions options) {
      this.options = options;
      this.inputSchema =
        new Schema.Parser().parse(options.getSchema());
      this.encryptedSchema =
        DeIdentifiedRecordSchemaConverter
          .withOriginalSchema(inputSchema)
          .withEncryptColumnKeys(options.getTokenizeColumns())
          .updatedSchema();
      this.tinkKeySetJson = options.getTinkEncryptionKeySetJson();
      this.mainKeyKmsUri = options.getMainKmsKeyUri();
      this.pipeline = Pipeline.create(options);
    }

    public static EncryptingPipelineFactory using(EncryptingPipelineOptions options) {
      return new EncryptingPipelineFactory(options);
    }

    public Pipeline buildPipeline() throws GeneralSecurityException, IOException {

      TupleTag<FlatRecord> flatRecordsTag = new TupleTag<>();

      pipeline
        .apply("Read" + SourceNames.forType(options.getSourceType()).asCamelCase(),
          TransformingFileReader
            .forSourceType(options.getSourceType())
            .from(options.getInputPattern())
            .withRecordsTag(flatRecordsTag))
        .get(flatRecordsTag)
        .apply("EncryptRecords",
          ValueEncryptionTransform.builder()
            .encryptColumnNames(ImmutableSet.copyOf(options.getTokenizeColumns()))
            .encryptedSchema(encryptedSchema)
            .valueTokenizerFactory(
              new DaeadEncryptingValueTokenizerFactory(buildClearEncryptionKeyset()))
            .build())
        .apply("WriteAVRO",
          AvroIO
            .writeGenericRecords(encryptedSchema)
            .withSuffix(".avro")
            .to(cleanDirectoryString(options.getOutputDirectory()) + "/data")
            .withCodec(CodecFactory.snappyCodec()));

      return pipeline;
    }

    /**
     * Unwraps the provided tink-encryption key using Cloud KMS Key-encryption-key.
     *
     * @return the plaintext encryption key-set
     * @throws GeneralSecurityException when provided wrapped Key-set is invalid.
     * @throws IOException              when there is error reading from the GCP-KMS.
     */
    private String buildClearEncryptionKeyset() throws GeneralSecurityException, IOException {
      KeysetHandle tinkKeySet =
        KeysetHandle.read(
          JsonKeysetReader.withString(tinkKeySetJson),
          new GcpKmsClient().withDefaultCredentials().getAead(mainKeyKmsUri));

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      CleartextKeysetHandle.write(tinkKeySet, JsonKeysetWriter.withOutputStream(baos));
      return baos.toString();
    }

    /**
     * Returns a directory path string without trailing <pre>/</pre>.
     */
    private static String cleanDirectoryString(String directory) {
      return checkNotNull(directory).replaceAll("(/)$", "");
    }
  }
}
