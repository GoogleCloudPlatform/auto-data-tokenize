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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Boolean.logicalXor;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.common.DeIdentifiedRecordSchemaConverter;
import com.google.cloud.solutions.autotokenize.common.DeidentifyColumns;
import com.google.cloud.solutions.autotokenize.common.RecordNester;
import com.google.cloud.solutions.autotokenize.common.SourceNames;
import com.google.cloud.solutions.autotokenize.common.TransformingFileReader;
import com.google.cloud.solutions.autotokenize.common.util.JsonConvertor;
import com.google.cloud.solutions.autotokenize.pipeline.dlp.BatchAndDlpDeIdRecords;
import com.google.cloud.solutions.autotokenize.pipeline.dlp.DlpClientFactory;
import com.google.cloud.solutions.autotokenize.pipeline.encryptors.DaeadEncryptingValueTokenizerFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
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
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

/**
 * Creates and launches a Dataflow pipeline to encrypt the provided input files using the
 * column-names from the FileDlpReport.
 */
public class EncryptionPipeline {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static void main(String[] args) throws Exception {

    EncryptingPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).as(EncryptingPipelineOptions.class);

    DataflowPipelineJob encryptingJob =
      (DataflowPipelineJob) EncryptingPipelineFactory
        .using(options)
        .buildPipeline()
        .run();

    logger.atInfo().log(
      "JobLink: https://console.cloud.google.com/dataflow/jobs/%s?project=%s",
      encryptingJob.getJobId(),
      encryptingJob.getProjectId());
  }

  /**
   * The pipeline creator.
   */
  @VisibleForTesting
  static class EncryptingPipelineFactory {

    private final EncryptingPipelineOptions options;
    private final Schema inputSchema;
    private final Schema encryptedSchema;
    private final Pipeline pipeline;
    private final String tinkKeySetJson;
    private final String mainKeyKmsUri;
    private final DlpEncryptConfig dlpEncryptConfig;
    private final DlpClientFactory dlpClientFactory;

    EncryptingPipelineFactory(EncryptingPipelineOptions options, Pipeline pipeline, DlpClientFactory dlpClientFactory) {
      this.options = checkValid(options);
      this.inputSchema =
        new Schema.Parser().parse(options.getSchema());
      this.encryptedSchema =
        DeIdentifiedRecordSchemaConverter
          .withOriginalSchema(inputSchema)
          .withEncryptColumnKeys(options.getTokenizeColumns())
          .updatedSchema();
      this.tinkKeySetJson = options.getTinkEncryptionKeySetJson();
      this.mainKeyKmsUri = options.getMainKmsKeyUri();
      this.dlpEncryptConfig =
        isNotBlank(options.getDlpEncryptConfigJson()) ?
          JsonConvertor.parseJson(options.getDlpEncryptConfigJson(), DlpEncryptConfig.class) :
          null;

      this.pipeline = pipeline;
      this.dlpClientFactory = dlpClientFactory;
    }

    public static EncryptingPipelineFactory using(EncryptingPipelineOptions options) {
      return new EncryptingPipelineFactory(options, Pipeline.create(checkValid(options)), DlpClientFactory.defaultFactory());
    }

    private static EncryptingPipelineOptions checkValid(EncryptingPipelineOptions options) {
      checkArgument(
        options.getTokenizeColumns() != null && !options.getTokenizeColumns().isEmpty(),
        "No columns to tokenize%n");

      checkArgument(
        isNotBlank(options.getOutputDirectory()) || isNotBlank(options.getOutputBigQueryTable()),
        "No output defined.%nProvide a GCS or BigQuery output");


      boolean isEncryption = isNotBlank(options.getMainKmsKeyUri()) && isNotBlank(options.getTinkEncryptionKeySetJson());
      boolean isDlpDeid = isNotBlank(options.getDlpEncryptConfigJson());

      checkArgument(
        logicalXor(isEncryption, isDlpDeid),
        "Provide one of Tink & KMS key or DlpDeidentifyConfig.%nFound both or none.");

      if (isDlpDeid) {
        // Validate DeidConfig is valid
        DlpEncryptConfig encryptConfig = JsonConvertor.parseJson(options.getDlpEncryptConfigJson(), DlpEncryptConfig.class);

        Sets.SetView<String> columnDiff =
          Sets.symmetricDifference(
            ImmutableSet.copyOf(options.getTokenizeColumns()),
            ImmutableSet.copyOf(DeidentifyColumns.columnNamesIn(encryptConfig)));

        checkArgument(
          columnDiff.isEmpty(),
          "DlpEncrypt config does not contain all tokenize columns.%nDifference: %s", columnDiff.toString());
      }

      return options;
    }

    Pipeline buildPipeline() throws Exception {

      TupleTag<FlatRecord> flatRecordsTag = new TupleTag<>();

      PCollection<GenericRecord> encryptedRecords =
        pipeline
          .apply("Read" + SourceNames.forType(options.getSourceType()).asCamelCase(),
            TransformingFileReader
              .forSourceType(options.getSourceType())
              .from(options.getInputPattern())
              .withRecordsTag(flatRecordsTag))
          .get(flatRecordsTag)
          .apply((dlpEncryptConfig != null) ? dlpDeidentify() : tinkEncryption())
          .apply(RecordNester.forSchema(encryptedSchema));

      if (options.getOutputDirectory() != null) {
        encryptedRecords
          .apply("WriteAVRO",
            AvroIO
              .writeGenericRecords(encryptedSchema)
              .withSuffix(".avro")
              .to(cleanDirectoryString(options.getOutputDirectory()) + "/data")
              .withCodec(CodecFactory.snappyCodec()));
      }

      if (options.getOutputBigQueryTable() != null) {

        encryptedRecords
          .apply(
            "WriteToBigQuery",
            BigQueryIO.
              <GenericRecord>write()
              .to(options.getOutputBigQueryTable())
              .useBeamSchema()
              .withWriteDisposition(WRITE_TRUNCATE));
      }

      return pipeline;
    }

    private BatchAndDlpDeIdRecords dlpDeidentify() {
      return
        BatchAndDlpDeIdRecords
          .withEncryptConfig(dlpEncryptConfig)
          .withDlpClientFactory(dlpClientFactory)
          .withDlpProjectId(options.getProject());
    }

    private ValueEncryptionTransform tinkEncryption() throws Exception {
      return
        ValueEncryptionTransform.builder()
          .encryptColumnNames(options.getTokenizeColumns())
          .valueTokenizerFactory(new DaeadEncryptingValueTokenizerFactory(buildClearEncryptionKeyset()))
          .build();
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
