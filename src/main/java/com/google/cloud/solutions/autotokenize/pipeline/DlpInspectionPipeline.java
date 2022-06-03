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

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.InspectionReport;
import com.google.cloud.solutions.autotokenize.auth.AccessTokenCredentialsFactory;
import com.google.cloud.solutions.autotokenize.common.InspectionReportFileWriter;
import com.google.cloud.solutions.autotokenize.common.InspectionReportToTableRow;
import com.google.cloud.solutions.autotokenize.common.SecretsClient;
import com.google.cloud.solutions.autotokenize.common.SourceNames;
import com.google.cloud.solutions.autotokenize.common.TransformingReader;
import com.google.cloud.solutions.autotokenize.datacatalog.DataCatalogWriter;
import com.google.cloud.solutions.autotokenize.datacatalog.MakeDataCatalogItems;
import com.google.cloud.solutions.autotokenize.dlp.BatchColumnsForDlp;
import com.google.cloud.solutions.autotokenize.dlp.DlpBatchInspectFactory;
import com.google.cloud.solutions.autotokenize.dlp.DlpClientFactory;
import com.google.cloud.solutions.autotokenize.dlp.DlpIdentify;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.Table;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

import java.time.Clock;

import static com.google.common.base.Preconditions.*;
import static com.google.common.collect.ImmutableSet.*;
import static org.apache.beam.sdk.io.FileIO.Write.*;
import static org.apache.commons.lang3.StringUtils.*;

/** Instantiates the sampling Dataflow pipeline based on provided options. */
public final class DlpInspectionPipeline {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final DlpInspectionOptions options;

  private final Pipeline pipeline;

  private final DlpClientFactory dlpClientFactory;

  private final SecretsClient secretsClient;

  private final Clock clock;

  private DlpInspectionPipeline(DlpInspectionOptions options) {
    this(
        options,
        Pipeline.create(options),
        DlpClientFactory.defaultFactory(),
        SecretsClient.of(),
        Clock.systemUTC());
  }

  @VisibleForTesting
  DlpInspectionPipeline(
      DlpInspectionOptions options,
      Pipeline pipeline,
      DlpClientFactory dlpClientFactory,
      SecretsClient secretsClient,
      Clock clock) {
    this.options = options;
    this.pipeline = pipeline;
    this.dlpClientFactory = checkNotNull(dlpClientFactory);
    this.secretsClient = checkNotNull(secretsClient);
    this.clock = checkNotNull(clock);

    validateOptions();
  }

  /** Creates the pipeline and applies the transforms. */
  @VisibleForTesting
  Pipeline makePipeline() {
    TupleTag<FlatRecord> recordsTag = new TupleTag<>();
    TupleTag<String> avroSchemaTag = new TupleTag<>();

    PCollectionTuple recordSchemaTuple =
        pipeline.apply(
            "Read" + SourceNames.forType(options.getSourceType()).asCamelCase(),
            TransformingReader.forSourceType(options.getSourceType())
                .from(options.getInputPattern())
                .withJdbcConfiguration(
                    JdbcConfigurationExtractor.using(options).jdbcConfiguration())
                .withSecretsClient(secretsClient)
                .withRecordsTag(recordsTag)
                .withAvroSchemaTag(avroSchemaTag));

    // Sample and Identify columns
    var columnInfoTag = new TupleTag<ColumnInformation>();
    var errorTag = new TupleTag<KV<ShardedKey<String>, Table>>();

    var dlpInspectResults =
        recordSchemaTuple
            .get(recordsTag)
            .apply("RandomColumnarSample", RandomColumnarSampler.any(options.getSampleSize()))
            .apply("BatchForDlp", new BatchColumnsForDlp())
            .apply(
                "DlpIdentify",
                DlpIdentify.builder()
                    .batchIdentifierFactory(makeDlpBatchIdentifierFactory())
                    .columnInfoTag(columnInfoTag)
                    .errorTag(errorTag)
                    .build());

    dlpInspectResults
        .get(errorTag)
        .setCoder(KvCoder.of(ShardedKey.Coder.of(StringUtf8Coder.of()), ProtoCoder.of(Table.class)))
        .apply("MakeErrorTableJson", ParDo.of(new ConvertTableToJsonFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply(
            "WriteErrorElements",
            FileIO.<String, KV<String, String>>writeDynamic()
                .via(Contextful.fn(KV::getValue), Contextful.fn(col -> TextIO.sink()))
                .by(KV::getKey)
                .withDestinationCoder(StringUtf8Coder.of())
                .withNaming(
                    Contextful.fn(
                        colName ->
                            defaultNaming(
                                /*prefix=*/ String.format(
                                        "col-%s", colName.replaceAll("[\\.\\$\\[\\]]+", "-"))
                                    .replaceAll("[-]+", "-"),
                                /*suffix=*/ ".json")))
                .to(options.getReportLocation() + "/error"));

    var inspectionReport =
        dlpInspectResults
            .get(columnInfoTag)
            .apply(
                "ExtractReport",
                MakeInspectionReport.builder()
                    .setAvroSchema(recordSchemaTuple.get(avroSchemaTag).apply(View.asSingleton()))
                    .setSourceType(options.getSourceType())
                    .setClock(clock)
                    .setInputPattern(options.getInputPattern())
                    .setJdbcConfiguration(
                        JdbcConfigurationExtractor.using(options).jdbcConfiguration())
                    .build());

    recordSchemaTuple
        .get(avroSchemaTag)
        .apply(
            "WriteSchema",
            TextIO.write()
                .to(options.getReportLocation() + "/schema")
                .withSuffix(".json")
                .withoutSharding());

    writeReportToGcs(inspectionReport);
    writeReportToBigQuery(inspectionReport);
    writeReportToDataCatalog(inspectionReport);

    return pipeline;
  }

  private void writeReportToDataCatalog(PCollection<InspectionReport> inspectionReport) {
    if (isNotBlank(options.getDataCatalogInspectionTagTemplateId())) {
      inspectionReport
          .apply(
              "ExtractDataCatalogItems",
              MakeDataCatalogItems.create(options.getDataCatalogInspectionTagTemplateId()))
          .apply(
              "WriteItemsToCatalog",
              DataCatalogWriter.builder()
                  .setEntryGroupId(options.getDataCatalogEntryGroupId())
                  .setInspectionTagTemplateId(options.getDataCatalogInspectionTagTemplateId())
                  .setForceUpdate(options.getDataCatalogForcedUpdate())
                  .build());
    }
  }

  private void writeReportToBigQuery(PCollection<InspectionReport> inspectionReport) {
    if (isNotBlank(options.getReportBigQueryTable())) {
      inspectionReport.apply(
          "WriteBigQueryReport",
          BigQueryIO.<InspectionReport>write()
              .to(options.getReportBigQueryTable())
              .withCreateDisposition(CreateDisposition.CREATE_NEVER)
              .withWriteDisposition(WriteDisposition.WRITE_APPEND)
              .withFormatFunction(InspectionReportToTableRow.create()));
    }
  }

  private void writeReportToGcs(PCollection<InspectionReport> inspectionReport) {
    if (isNotBlank(options.getReportLocation())) {
      inspectionReport.apply(
          "WriteReportToGcs", InspectionReportFileWriter.create(options.getReportLocation()));
    }
  }

  private void validateOptions() {
    checkArgument(
        isNotBlank(options.getReportLocation()) || isNotBlank(options.getReportBigQueryTable()),
        "Provide at least one of --reportLocation or --reportBigQueryTable");

    switch (options.getSourceType()) {
      case JDBC_QUERY:
      case JDBC_TABLE:
        checkArgument(
            JdbcConfigurationExtractor.using(options).jdbcConfiguration() != null,
            "JDBCConfiguration missing for source-type");
        // For JDBC there is additional check for entry group id.
        checkArgument(
            options.getDataCatalogInspectionTagTemplateId() == null
                || options.getDataCatalogEntryGroupId() != null,
            "provide valid entry group id for JDBC, AVRO or PARQUET source.");
        break;

      case AVRO:
      case PARQUET:
        checkArgument(
            options.getDataCatalogInspectionTagTemplateId() == null
                || options.getDataCatalogEntryGroupId() != null,
            "provide valid entry group id for JDBC, AVRO or PARQUET source.");
        break;

      case BIGQUERY_TABLE:
      case BIGQUERY_QUERY:
        break;

      case UNRECOGNIZED:
      case UNKNOWN_FILE_TYPE:
      default:
        throw new IllegalArgumentException("" + options.getSourceType() + " is unsupported.");
    }
  }

  /**
   * Returns a factory to create {@link DlpServiceClient} instance using {@code
   * DlpServiceClient.create()} method.
   */
  private DlpBatchInspectFactory makeDlpBatchIdentifierFactory() {

    var dlpIdentifierBuilder =
        DlpBatchInspectFactory.builder()
            .projectId(options.getProject())
            .dlpRegion(options.getDlpRegion())
            .dlpFactory(dlpClientFactory);

    if (options.getObservableInfoTypes() != null && !options.getObservableInfoTypes().isEmpty()) {
      ImmutableSet<InfoType> userProvidedInfoTypes =
          options.getObservableInfoTypes().stream()
              .map(InfoType.newBuilder()::setName)
              .map(InfoType.Builder::build)
              .collect(toImmutableSet());

      dlpIdentifierBuilder.observableType(userProvidedInfoTypes);
      dlpIdentifierBuilder.observableType(userProvidedInfoTypes);
    }

    return dlpIdentifierBuilder.build();
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(DlpInspectionOptions.class);
    DlpInspectionOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DlpInspectionOptions.class);
    options.setCredentialFactoryClass(AccessTokenCredentialsFactory.class);
    options.setJobName(new UserEnvironmentOptions.JobNameFactory().create(options));
    logger.atInfo().log("Staging the Dataflow job");
    new DlpInspectionPipeline(options).makePipeline().run();
  }

  private static class ConvertTableToJsonFn
      extends DoFn<KV<ShardedKey<String>, Table>, KV<String, String>> {

    @ProcessElement
    public void makeProtoJson(
        @Element KV<ShardedKey<String>, Table> element,
        OutputReceiver<KV<String, String>> outputReceiver) {
      try {
        outputReceiver.output(
            KV.of(element.getKey().getKey(), JsonFormat.printer().print(element.getValue())));
      } catch (InvalidProtocolBufferException ex) {
        GoogleLogger.forEnclosingClass().atSevere().withCause(ex).log();
      }
    }
  }
}
