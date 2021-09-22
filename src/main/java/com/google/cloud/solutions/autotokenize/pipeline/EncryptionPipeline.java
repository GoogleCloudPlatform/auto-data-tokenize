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
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.common.CsvRowFlatRecordConvertors;
import com.google.cloud.solutions.autotokenize.common.DeIdentifiedRecordSchemaConverter;
import com.google.cloud.solutions.autotokenize.common.DeidentifyColumns;
import com.google.cloud.solutions.autotokenize.common.JsonConvertor;
import com.google.cloud.solutions.autotokenize.common.RecordNester;
import com.google.cloud.solutions.autotokenize.common.SecretsClient;
import com.google.cloud.solutions.autotokenize.common.SourceNames;
import com.google.cloud.solutions.autotokenize.common.TransformingReader;
import com.google.cloud.solutions.autotokenize.dlp.BatchAndDlpDeIdRecords;
import com.google.cloud.solutions.autotokenize.dlp.DlpClientFactory;
import com.google.cloud.solutions.autotokenize.encryptors.ClearTextKeySetExtractor;
import com.google.cloud.solutions.autotokenize.encryptors.DaeadEncryptingValueTokenizerFactory;
import com.google.cloud.solutions.autotokenize.encryptors.GcpKmsClearTextKeySetExtractor;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Creates and launches a Dataflow pipeline to encrypt the provided input files using the
 * column-names from the FileDlpReport.
 */
public class EncryptionPipeline {

  private final EncryptionPipelineOptions options;
  protected final Pipeline pipeline;
  private final DlpClientFactory dlpClientFactory;
  private final SecretsClient secretsClient;
  private final ClearTextKeySetExtractor keySetExtractor;

  public static void main(String[] args) throws Exception {

    EncryptionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(EncryptionPipelineOptions.class);

    new EncryptionPipeline(
            options,
            Pipeline.create(options),
            DlpClientFactory.defaultFactory(),
            SecretsClient.of())
        .run();
  }

  @VisibleForTesting
  EncryptionPipeline(
      EncryptionPipelineOptions options,
      Pipeline pipeline,
      DlpClientFactory dlpClientFactory,
      SecretsClient secretsClient,
      ClearTextKeySetExtractor keySetExtractor) {
    this.options = options;
    this.pipeline = pipeline;
    this.dlpClientFactory = dlpClientFactory;
    this.secretsClient = secretsClient;
    this.keySetExtractor = keySetExtractor;
  }

  public EncryptionPipeline(
      EncryptionPipelineOptions options,
      Pipeline pipeline,
      DlpClientFactory dlpClientFactory,
      SecretsClient secretsClient) {
    this(
        options,
        pipeline,
        dlpClientFactory,
        secretsClient,
        new GcpKmsClearTextKeySetExtractor(
            options.getTinkEncryptionKeySetJson(), options.getMainKmsKeyUri()));
  }

  public PipelineResult run() throws Exception {
    var encryptedSchema = buildEncryptedSchema();
    var encryptedRecords =
        applyReadAndEncryptionSteps().apply(RecordNester.forSchema(encryptedSchema));

    if (options.getOutputDirectory() != null) {
      encryptedRecords.apply(
          "WriteAVRO",
          AvroIO.writeGenericRecords(encryptedSchema)
              .withSuffix(".avro")
              .to(cleanDirectoryString(options.getOutputDirectory()) + "/data")
              .withCodec(CodecFactory.snappyCodec()));
    }

    if (options.getOutputBigQueryTable() != null) {

      encryptedRecords
          .apply("Reshuffle", Reshuffle.viaRandomKey())
          .setCoder(AvroUtils.schemaCoder(GenericRecord.class, encryptedSchema))
          .apply(
              "WriteToBigQuery",
              BigQueryIO.<GenericRecord>write()
                  .to(options.getOutputBigQueryTable())
                  .useBeamSchema()
                  .optimizedWrites()
                  .withWriteDisposition(WRITE_TRUNCATE));
    }

    return pipeline.run();
  }

  protected PCollection<FlatRecord> applyReadAndEncryptionSteps() throws Exception {
    return new EncryptingPipelineFactory(
            options, pipeline, dlpClientFactory, secretsClient, keySetExtractor)
        .makeEncryptingSteps();
  }

  private Schema buildEncryptedSchema() {
    checkArgument(
        isNotBlank(options.getSchema())
            || (SourceType.CSV_FILE.equals(options.getSourceType())
                && options.getCsvHeaders() != null
                && !options.getCsvHeaders().isEmpty()),
        "Provide Source's Avro Schema or headers for CSV_FILE.");

    var inputSchema =
        (options.getSchema() != null)
            ? new Schema.Parser().parse(options.getSchema())
            : CsvRowFlatRecordConvertors.makeCsvAvroSchema(options.getCsvHeaders());

    List<String> tokenizeColumnNames =
        (options.getDlpEncryptConfigJson() == null)
            ?
            // Use provided tokenizeColumnNames
            options.getTokenizeColumns()
            :
            // For DLP Tokenize use columnNames from config
            DeidentifyColumns.columnNamesIn(
                JsonConvertor.parseJson(options.getDlpEncryptConfigJson(), DlpEncryptConfig.class));

    return DeIdentifiedRecordSchemaConverter.withOriginalSchema(inputSchema)
        .withEncryptColumnKeys(tokenizeColumnNames)
        .updatedSchema();
  }

  /** Returns a directory path string without trailing {@code /}. */
  private static String cleanDirectoryString(String directory) {
    return checkNotNull(directory).replaceAll("(/)$", "");
  }

  /** The encryption pipeline creator. */
  private static class EncryptingPipelineFactory {

    private final EncryptionPipelineOptions options;
    private final Pipeline pipeline;
    private final DlpEncryptConfig dlpEncryptConfig;
    private final DlpClientFactory dlpClientFactory;
    private final SecretsClient secretsClient;
    private final ClearTextKeySetExtractor keySetExtractor;

    EncryptingPipelineFactory(
        EncryptionPipelineOptions options,
        Pipeline pipeline,
        DlpClientFactory dlpClientFactory,
        SecretsClient secretsClient,
        ClearTextKeySetExtractor keySetExtractor) {
      this.options = checkValid(options);
      this.dlpEncryptConfig =
          isNotBlank(options.getDlpEncryptConfigJson())
              ? JsonConvertor.parseJson(options.getDlpEncryptConfigJson(), DlpEncryptConfig.class)
              : null;

      this.pipeline = pipeline;
      this.dlpClientFactory = dlpClientFactory;
      this.secretsClient = checkNotNull(secretsClient);
      this.keySetExtractor = keySetExtractor;
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

    protected PCollection<FlatRecord> makeEncryptingSteps()
        throws GeneralSecurityException, IOException {
      TupleTag<FlatRecord> flatRecordsTag = new TupleTag<>();

      return pipeline
          .apply(
              "Read" + SourceNames.forType(options.getSourceType()).asCamelCase(),
              TransformingReader.forSourceType(options.getSourceType())
                  .from(options.getInputPattern())
                  .withJdbcConfiguration(
                      JdbcConfigurationExtractor.using(options).jdbcConfiguration())
                  .withSecretsClient(secretsClient)
                  .withRecordsTag(flatRecordsTag)
                  .withCsvHeaders(options.getCsvHeaders())
                  .withCsvFirstRowHeader(options.getCsvFirstRowHeader()))
          .get(flatRecordsTag)
          .apply((dlpEncryptConfig != null) ? dlpDeidentify() : tinkEncryption());
    }

    private BatchAndDlpDeIdRecords dlpDeidentify() {
      return BatchAndDlpDeIdRecords.withEncryptConfig(dlpEncryptConfig)
          .withDlpClientFactory(dlpClientFactory)
          .withDlpProjectId(options.getProject());
    }

    private ValueEncryptionTransform tinkEncryption() throws GeneralSecurityException, IOException {
      return ValueEncryptionTransform.builder()
          .encryptColumnNames(options.getTokenizeColumns())
          .valueTokenizerFactory(new DaeadEncryptingValueTokenizerFactory(keySetExtractor.get()))
          .build();
    }
  }
}
