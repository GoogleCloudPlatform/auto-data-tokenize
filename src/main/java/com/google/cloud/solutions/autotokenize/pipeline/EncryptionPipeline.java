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
import com.google.cloud.solutions.autotokenize.common.JsonConvertor;
import com.google.cloud.solutions.autotokenize.common.RecordNester;
import com.google.cloud.solutions.autotokenize.common.SecretsClient;
import com.google.cloud.solutions.autotokenize.common.SourceNames;
import com.google.cloud.solutions.autotokenize.common.TransformingReader;
import com.google.cloud.solutions.autotokenize.dlp.BatchAndDlpDeIdRecords;
import com.google.cloud.solutions.autotokenize.dlp.DlpClientFactory;
import com.google.cloud.solutions.autotokenize.encryptors.DaeadEncryptingValueTokenizerFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Creates and launches a Dataflow pipeline to encrypt the provided input files using the
 * column-names from the FileDlpReport.
 */
public class EncryptionPipeline {

  public static void main(String[] args) throws GeneralSecurityException, IOException {

    EncryptionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(EncryptionPipelineOptions.class);

    EncryptingPipelineFactory.using(options).buildPipeline().run();
  }

  /** The pipeline creator. */
  @VisibleForTesting
  static class EncryptingPipelineFactory {

    private final EncryptionPipelineOptions options;
    private final Schema inputSchema;
    private final Schema encryptedSchema;
    private final Pipeline pipeline;
    private final String tinkKeySetJson;
    private final String mainKeyKmsUri;
    private final DlpEncryptConfig dlpEncryptConfig;
    private final DlpClientFactory dlpClientFactory;
    private final SecretsClient secretsClient;
    private final String clearTextEncryptionKeySet;

    EncryptingPipelineFactory(
        EncryptionPipelineOptions options,
        Pipeline pipeline,
        DlpClientFactory dlpClientFactory,
        SecretsClient secretsClient,
        String clearTextEncryptionKeySet)
        throws GeneralSecurityException, IOException {
      this.options = checkValid(options);
      this.inputSchema = new Schema.Parser().parse(options.getSchema());

      this.tinkKeySetJson = options.getTinkEncryptionKeySetJson();
      this.mainKeyKmsUri = options.getMainKmsKeyUri();
      this.dlpEncryptConfig =
          isNotBlank(options.getDlpEncryptConfigJson())
              ? JsonConvertor.parseJson(options.getDlpEncryptConfigJson(), DlpEncryptConfig.class)
              : null;

      List<String> tokenizeColumnNames =
          (this.dlpEncryptConfig == null)
              ?
              // Use provided tokenizeColumnNames
              options.getTokenizeColumns()
              :
              // For DLP Tokenize use columnNames from config
              DeidentifyColumns.columnNamesIn(dlpEncryptConfig);

      this.encryptedSchema =
          DeIdentifiedRecordSchemaConverter.withOriginalSchema(inputSchema)
              .withEncryptColumnKeys(tokenizeColumnNames)
              .updatedSchema();

      this.pipeline = pipeline;
      this.dlpClientFactory = dlpClientFactory;
      this.secretsClient = checkNotNull(secretsClient);

      this.clearTextEncryptionKeySet =
          (options.getDlpEncryptConfigJson() == null && clearTextEncryptionKeySet == null)
              ? buildClearEncryptionKeyset(tinkKeySetJson, mainKeyKmsUri)
              : clearTextEncryptionKeySet;
    }

    public static EncryptingPipelineFactory using(EncryptionPipelineOptions options)
        throws GeneralSecurityException, IOException {
      return new EncryptingPipelineFactory(
          options,
          Pipeline.create(checkValid(options)),
          DlpClientFactory.defaultFactory(),
          SecretsClient.of(),
          null);
    }

    private static EncryptionPipelineOptions checkValid(EncryptionPipelineOptions options) {
      checkArgument(
          (options.getTokenizeColumns() != null && !options.getTokenizeColumns().isEmpty())
              || options.getDlpEncryptConfigJson() != null,
          "No columns to tokenize");

      checkArgument(
          isNotBlank(options.getOutputDirectory()) || isNotBlank(options.getOutputBigQueryTable()),
          "No output defined.%nProvide a GCS or BigQuery output");

      boolean isEncryption =
          isNotBlank(options.getMainKmsKeyUri())
              && isNotBlank(options.getTinkEncryptionKeySetJson());
      boolean isDlpDeid = isNotBlank(options.getDlpEncryptConfigJson());

      checkArgument(
          logicalXor(isEncryption, isDlpDeid),
          "Provide one of Tink & KMS key or DlpDeidentifyConfig.\nFound both or none.");

      if (isDlpDeid) {
        // Validate DeidConfig is valid
        DlpEncryptConfig encryptConfig =
            JsonConvertor.parseJson(options.getDlpEncryptConfigJson(), DlpEncryptConfig.class);

        checkArgument(
            !DeidentifyColumns.columnNamesIn(encryptConfig).isEmpty(),
            "DlpEncrypt config does not contain any tokenize columns.");
      }

      return options;
    }

    Pipeline buildPipeline() {

      TupleTag<FlatRecord> flatRecordsTag = new TupleTag<>();

      PCollection<GenericRecord> encryptedRecords =
          pipeline
              .apply(
                  "Read" + SourceNames.forType(options.getSourceType()).asCamelCase(),
                  TransformingReader.forSourceType(options.getSourceType())
                      .from(options.getInputPattern())
                      .withJdbcConfiguration(
                          JdbcConfigurationExtractor.using(options).jdbcConfiguration())
                      .withSecretsClient(secretsClient)
                      .withRecordsTag(flatRecordsTag))
              .get(flatRecordsTag)
              .apply((dlpEncryptConfig != null) ? dlpDeidentify() : tinkEncryption())
              .apply(RecordNester.forSchema(encryptedSchema));

      if (options.getOutputDirectory() != null) {
        encryptedRecords.apply(
            "WriteAVRO",
            AvroIO.writeGenericRecords(encryptedSchema)
                .withSuffix(".avro")
                .to(cleanDirectoryString(options.getOutputDirectory()) + "/data")
                .withCodec(CodecFactory.snappyCodec()));
      }

      if (options.getOutputBigQueryTable() != null) {

        encryptedRecords.apply(
            "WriteToBigQuery",
            BigQueryIO.<GenericRecord>write()
                .to(options.getOutputBigQueryTable())
                .useBeamSchema()
                .withWriteDisposition(WRITE_TRUNCATE));
      }

      return pipeline;
    }

    private BatchAndDlpDeIdRecords dlpDeidentify() {
      return BatchAndDlpDeIdRecords.withEncryptConfig(dlpEncryptConfig)
          .withDlpClientFactory(dlpClientFactory)
          .withDlpProjectId(options.getProject());
    }

    private ValueEncryptionTransform tinkEncryption() {
      return ValueEncryptionTransform.builder()
          .encryptColumnNames(options.getTokenizeColumns())
          .valueTokenizerFactory(
              new DaeadEncryptingValueTokenizerFactory(clearTextEncryptionKeySet))
          .build();
    }

    /**
     * Unwraps the provided tink-encryption key using Cloud KMS Key-encryption-key.
     *
     * @return the plaintext encryption key-set
     * @throws GeneralSecurityException when provided wrapped Key-set is invalid.
     * @throws IOException when there is error reading from the GCP-KMS.
     */
    private static String buildClearEncryptionKeyset(String tinkKeySetJson, String mainKeyKmsUri)
        throws GeneralSecurityException, IOException {
      var tinkKeySet =
          KeysetHandle.read(
              JsonKeysetReader.withString(tinkKeySetJson),
              new GcpKmsClient().withDefaultCredentials().getAead(mainKeyKmsUri));

      var baos = new ByteArrayOutputStream();
      CleartextKeysetHandle.write(tinkKeySet, JsonKeysetWriter.withOutputStream(baos));
      return baos.toString();
    }

    /** Returns a directory path string without trailing {@code /}. */
    private static String cleanDirectoryString(String directory) {
      return checkNotNull(directory).replaceAll("(/)$", "");
    }
  }
}
