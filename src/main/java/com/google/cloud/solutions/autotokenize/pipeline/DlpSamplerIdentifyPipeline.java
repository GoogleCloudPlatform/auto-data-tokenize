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
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.beam.sdk.io.FileIO.Write.defaultNaming;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.InspectionReport;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.JdbcConfiguration;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.common.InspectionReportToTableRow;
import com.google.cloud.solutions.autotokenize.common.SourceNames;
import com.google.cloud.solutions.autotokenize.common.TransformingReader;
import com.google.cloud.solutions.autotokenize.common.util.JsonConvertor;
import com.google.cloud.solutions.autotokenize.datacatalog.DataCatalogWriter;
import com.google.cloud.solutions.autotokenize.datacatalog.MakeDataCatalogItems;
import com.google.cloud.solutions.autotokenize.dlp.BatchColumnsForDlp;
import com.google.cloud.solutions.autotokenize.dlp.DlpBatchInspectFactory;
import com.google.cloud.solutions.autotokenize.dlp.DlpClientFactory;
import com.google.cloud.solutions.autotokenize.dlp.DlpIdentify;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.privacy.dlp.v2.InfoType;
import java.time.Clock;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Instantiates the sampling Dataflow pipeline based on provided options. */
public final class DlpSamplerIdentifyPipeline {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final DlpSamplerIdentifyOptions options;

  private final Pipeline pipeline;

  private final DlpClientFactory dlpClientFactory;

  private final Clock clock;

  private DlpSamplerIdentifyPipeline(DlpSamplerIdentifyOptions options) {
    this(options, Pipeline.create(options), DlpClientFactory.defaultFactory(), Clock.systemUTC());
  }

  @VisibleForTesting
  DlpSamplerIdentifyPipeline(
      DlpSamplerIdentifyOptions options,
      Pipeline pipeline,
      DlpClientFactory dlpClientFactory,
      Clock clock) {
    this.options = options;
    this.pipeline = pipeline;
    this.dlpClientFactory = checkNotNull(dlpClientFactory);
    this.clock = checkNotNull(clock);

    validateOptions();
  }

  /** Creates the pipeline and applies the transforms. */
  @VisibleForTesting
  Pipeline makePipeline() {
    TupleTag<FlatRecord> recordsTag = new TupleTag<>();
    TupleTag<String> avroSchemaTag = new TupleTag<>();

    var jdbcConfiguration = extractJdbcConfiguration();

    PCollectionTuple recordSchemaTuple =
        pipeline.apply(
            "Read" + SourceNames.forType(options.getSourceType()).asCamelCase(),
            TransformingReader.forSourceType(options.getSourceType())
                .from(options.getInputPattern())
                .withJdbcConfiguration(jdbcConfiguration)
                .withRecordsTag(recordsTag)
                .withAvroSchemaTag(avroSchemaTag)
                .withSampleSize(options.getSampleSize()));

    // Sample and Identify columns
    var inspectionReport =
        recordSchemaTuple
            .get(recordsTag)
            .apply("RandomColumnarSample", RandomColumnarSampler.any(options.getSampleSize()))
            .apply("BatchForDlp", new BatchColumnsForDlp())
            .apply(
                "DlpIdentify", DlpIdentify.withIdentifierFactory(makeDlpBatchIdentifierFactory()))
            .apply(
                "ExtractReport",
                MakeInspectionReport.builder()
                    .setAvroSchema(recordSchemaTuple.get(avroSchemaTag).apply(View.asSingleton()))
                    .setSourceType(options.getSourceType())
                    .setClock(clock)
                    .setInputPattern(options.getInputPattern())
                    .setJdbcConfiguration(jdbcConfiguration)
                    .build());

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

      PCollection<String> avroSchema =
          inspectionReport.apply("ExtractSchema", MapElements.via(new SchemaExtractor()));

      PCollection<ColumnInformation> columnInformation =
          inspectionReport
              .apply("ExtractColumnInformation", MapElements.via(new ColumnInformationExtractor()))
              .apply(Flatten.iterables());

      // Write the schema to a file.
      avroSchema.apply(
          "WriteSchema",
          FileIO.<String>write()
              .to(options.getReportLocation())
              .via(TextIO.sink())
              .withNumShards(1)
              .withNaming((window, pane, numShards, shardIndex, compression) -> "schema.json"));

      // Write Column Information to GCS file.
      columnInformation.apply(
          "WriteColumnReport",
          FileIO.<String, ColumnInformation>writeDynamic()
              .via(
                  Contextful.fn(JsonConvertor::asJsonString),
                  Contextful.fn(colName -> TextIO.sink()))
              .by(ColumnInformation::getColumnName)
              .withDestinationCoder(StringUtf8Coder.of())
              .withNoSpilling()
              .withNaming(
                  Contextful.fn(
                      colName ->
                          defaultNaming(
                              /*prefix=*/ String.format(
                                      "col-%s", colName.replaceAll("[\\.\\$\\[\\]]+", "-"))
                                  .replaceAll("[-]+", "-"),
                              /*suffix=*/ ".json")))
              .to(options.getReportLocation()));
    }
  }

  private void validateOptions() {
    checkArgument(
        isNotBlank(options.getReportLocation()) || isNotBlank(options.getReportBigQueryTable()),
        "Provide at least one of --reportLocation or --reportBigQueryTable");

    switch (options.getSourceType()) {
      case JDBC_TABLE:
        checkArgument(
            extractJdbcConfiguration() != null, "JDBCConfiguration missing for source-type");
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

  @Nullable
  private JdbcConfiguration extractJdbcConfiguration() {
    if (options.getSourceType().equals(SourceType.JDBC_TABLE)) {

      checkArgument(
          isNotBlank(options.getJdbcConnectionUrl()) && isNotBlank(options.getJdbcDriverClass()),
          "Provide both jdbcDriverClass and jdbcConnectionUrl parameters.");

      return JdbcConfiguration.newBuilder()
          .setConnectionUrl(options.getJdbcConnectionUrl())
          .setDriverClassName(options.getJdbcDriverClass())
          .build();
    }

    return null;
  }

  /**
   * Returns a factory to create {@link DlpServiceClient} instance using {@code
   * DlpServiceClient.create()} method.
   */
  private DlpBatchInspectFactory makeDlpBatchIdentifierFactory() {

    var dlpIdentifierBuilder =
        DlpBatchInspectFactory.builder()
            .projectId(options.getProject())
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
    PipelineOptionsFactory.register(DlpSamplerIdentifyOptions.class);
    DlpSamplerIdentifyOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DlpSamplerIdentifyOptions.class);

    logger.atInfo().log("Staging the Dataflow job");

    new DlpSamplerIdentifyPipeline(options).makePipeline().run();
  }

  private static final class SchemaExtractor extends SimpleFunction<InspectionReport, String> {

    @Override
    public String apply(InspectionReport input) {
      return input.getAvroSchema();
    }
  }

  private static final class ColumnInformationExtractor
      extends SimpleFunction<InspectionReport, Iterable<ColumnInformation>> {

    @Override
    public Iterable<ColumnInformation> apply(InspectionReport input) {

      if (input.getColumnReportCount() == 0) {
        return ImmutableList.of();
      }

      return input.getColumnReportList();
    }
  }
}
