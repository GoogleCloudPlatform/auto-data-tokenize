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
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyManagementServiceSettings;
import com.google.cloud.secretmanager.v1.SecretManagerServiceSettings;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.KeyMaterialType;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.auth.AccessTokenCredentialsFactory;
import com.google.cloud.solutions.autotokenize.auth.GoogleCredentialsWithQuotaProjectId;
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
import com.google.cloud.solutions.autotokenize.encryptors.GcpKmsClearTextKeySetExtractor;
import com.google.cloud.solutions.autotokenize.encryptors.ValueTokenizerFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;

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
  private final KeyManagementServiceClient kmsClient;
  private final ClearTextKeySetExtractor keySetExtractor;

  public static void main(String[] args) throws Exception {

    EncryptionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(EncryptionPipelineOptions.class);

    options.setCredentialFactoryClass(AccessTokenCredentialsFactory.class);
    options.setJobName(new UserEnvironmentOptions.JobNameFactory().create(options));

    new EncryptionPipeline(
            options,
            Pipeline.create(options),
            DlpClientFactory.defaultFactory(),
            SecretsClient.withSettings(getSecretManagerServiceSettings(options)),
            KeyManagementServiceClient.create(KeyManagementServiceSettings.newBuilder().setQuotaProjectId(options.getProject()).build()))
        .run();
  }

  private static SecretManagerServiceSettings getSecretManagerServiceSettings(EncryptionPipelineOptions options) throws IOException, GeneralSecurityException {
    return SecretManagerServiceSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider
                    .create(AccessTokenCredentialsFactory.fromOptions(options).getCredential()))
            .setQuotaProjectId(options.getProject()).build();
  }

  @VisibleForTesting
  EncryptionPipeline(
      EncryptionPipelineOptions options,
      Pipeline pipeline,
      DlpClientFactory dlpClientFactory,
      SecretsClient secretsClient,
      KeyManagementServiceClient kmsClient,
      ClearTextKeySetExtractor keySetExtractor) {
    this.options = options;
    this.pipeline = pipeline;
    this.dlpClientFactory = dlpClientFactory;
    this.secretsClient = secretsClient;
    this.kmsClient = kmsClient;
    this.keySetExtractor = keySetExtractor;
  }

  public EncryptionPipeline(
      EncryptionPipelineOptions options,
      Pipeline pipeline,
      DlpClientFactory dlpClientFactory,
      SecretsClient secretsClient,
      KeyManagementServiceClient kmsClient) {
    this(
        options,
        pipeline,
        dlpClientFactory,
        secretsClient,
        kmsClient,
        new GcpKmsClearTextKeySetExtractor(
            options.getTinkEncryptionKeySetJson(), options.getMainKmsKeyUri(),
                new GoogleCredentialsWithQuotaProjectId((OAuth2Credentials) options.getGcpCredential())));
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
                  .withWriteDisposition(
                      (options.getBigQueryAppend()) ? WRITE_APPEND : WRITE_TRUNCATE));
    }

    return pipeline.run();
  }

  @VisibleForTesting
  PCollection<FlatRecord> applyReadAndEncryptionSteps() {
    return new EncryptingPipelineFactory(
            options, pipeline, dlpClientFactory, secretsClient, kmsClient, keySetExtractor)
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
    private final KeyManagementServiceClient kmsClient;
    private final ClearTextKeySetExtractor keySetExtractor;

    EncryptingPipelineFactory(
        EncryptionPipelineOptions options,
        Pipeline pipeline,
        DlpClientFactory dlpClientFactory,
        SecretsClient secretsClient,
        KeyManagementServiceClient kmsClient,
        ClearTextKeySetExtractor keySetExtractor) {
      this.options = checkValid(options);
      this.dlpEncryptConfig =
          isNotBlank(options.getDlpEncryptConfigJson())
              ? JsonConvertor.parseJson(options.getDlpEncryptConfigJson(), DlpEncryptConfig.class)
              : null;

      this.pipeline = pipeline;
      this.dlpClientFactory = dlpClientFactory;
      this.secretsClient = secretsClient;
      this.kmsClient = kmsClient;
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
          (isNotBlank(options.getMainKmsKeyUri())
                  && isNotBlank(options.getTinkEncryptionKeySetJson()))
              || (isNotBlank(options.getKeyMaterial()));
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

    protected PCollection<FlatRecord> makeEncryptingSteps() {
      var flatRecordsTag = new TupleTag<FlatRecord>();

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
          .withDlpProjectId(options.getProject())
          .withDlpRegion(options.getDlpRegion());
    }

    private ValueEncryptionTransform tinkEncryption() {
      return ValueEncryptionTransform.builder()
          .encryptColumnNames(options.getTokenizeColumns())
          .valueTokenizerFactory(buildValueTokenizerFactory())
          .build();
    }

    private ValueTokenizerFactory buildValueTokenizerFactory() {
      try {
        var encryptorFactoryClazz = Class.forName(options.getValueTokenizerFactoryFullClassName());

        checkArgument(
            ValueTokenizerFactory.class.isAssignableFrom(encryptorFactoryClazz),
            "Class %s does extend ValueTokenizerFactory",
            options.getValueTokenizerFactoryFullClassName());

        var ctor = encryptorFactoryClazz.getConstructor(String.class, KeyMaterialType.class);

        switch (options.getKeyMaterialType()) {
          case TINK_GCP_KEYSET_JSON:
            return (ValueTokenizerFactory)
                ctor.newInstance(keySetExtractor.get(), options.getKeyMaterialType());

          case TINK_GCP_KEYSET_JSON_FROM_SECRET_MANAGER:
            var encryptionKeySetJson = secretsClient.accessSecret(options.getKeyMaterial());
            ClearTextKeySetExtractor keySetExtractor2 =
                new GcpKmsClearTextKeySetExtractor(
                    encryptionKeySetJson, options.getMainKmsKeyUri(),
                        new GoogleCredentialsWithQuotaProjectId((OAuth2Credentials) options.getGcpCredential()));
            return (ValueTokenizerFactory)
                ctor.newInstance(keySetExtractor2.get(), KeyMaterialType.TINK_GCP_KEYSET_JSON);

          case RAW_BASE64_KEY:
          case RAW_UTF8_KEY:
            return (ValueTokenizerFactory)
                ctor.newInstance(options.getKeyMaterial(), options.getKeyMaterialType());

          case GCP_KMS_WRAPPED_KEY:
            var cipherKey =
                ByteString.copyFrom(BaseEncoding.base64().decode(options.getKeyMaterial()));
            var cleartextKey =
                kmsClient
                    .decrypt(options.getMainKmsKeyUri(), cipherKey)
                    .getPlaintext()
                    .toByteArray();

            return (ValueTokenizerFactory)
                ctor.newInstance(
                    BaseEncoding.base64().encode(cleartextKey), KeyMaterialType.RAW_BASE64_KEY);

          case GCP_SECRET_KEY:
            var encryptionKey = secretsClient.accessSecret(options.getKeyMaterial());
            return (ValueTokenizerFactory)
                ctor.newInstance(encryptionKey, KeyMaterialType.RAW_UTF8_KEY);

          case UNRECOGNIZED:
          case UNKNOWN_KEY_MATERIAL_TYPE:
            throw new IllegalArgumentException("unkknown keymaterial type");
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("Error creating encryptionValueFactory", e);
      }

      throw new RuntimeException("error in instantiating ValueTokenizerFactory");
    }
  }
}
