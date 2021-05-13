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
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.beam.sdk.io.FileIO.Write.defaultNaming;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.JdbcConfiguration;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.common.SourceNames;
import com.google.cloud.solutions.autotokenize.common.TransformingReader;
import com.google.cloud.solutions.autotokenize.common.util.JsonConvertor;
import com.google.cloud.solutions.autotokenize.pipeline.datacatalog.DataCatalogWriter;
import com.google.cloud.solutions.autotokenize.pipeline.datacatalog.ExtractDataCatalogItems;
import com.google.cloud.solutions.autotokenize.pipeline.dlp.BatchColumnsForDlp;
import com.google.cloud.solutions.autotokenize.pipeline.dlp.DlpClientFactory;
import com.google.cloud.solutions.autotokenize.pipeline.dlp.DlpIdentify;
import com.google.cloud.solutions.autotokenize.pipeline.dlp.DlpSenderFactory;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.privacy.dlp.v2.InfoType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
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

  private DlpSamplerIdentifyPipeline(DlpSamplerIdentifyOptions options) {
    this.options = options;
    this.pipeline = Pipeline.create(options);

    validateOptions();
  }

  /** Creates the pipeline and applies the transforms. */
  private Pipeline makePipeline() {
    TupleTag<FlatRecord> recordsTag = new TupleTag<>();
    TupleTag<String> avroSchemaTag = new TupleTag<>();

    PCollectionTuple recordSchemaTuple =
        pipeline.apply(
            "Read" + SourceNames.forType(options.getSourceType()).asCamelCase(),
            TransformingReader.forSourceType(options.getSourceType())
                .from(options.getInputPattern())
                .withJdbcConfiguration(extractJdbcConfiguration())
                .withRecordsTag(recordsTag)
                .withAvroSchemaTag(avroSchemaTag)
                .withSampleSize(options.getSampleSize()));

    // Write the schema to a file.
    recordSchemaTuple
        .get(avroSchemaTag)
        .apply(
            "WriteSchema",
            TextIO.write()
                .to(options.getReportLocation())
                .withoutSharding()
                .withSuffix("schema.json"));

    // Sample and Identify columns
    PCollection<ColumnInformation> columnInformation =
        recordSchemaTuple
            .get(recordsTag)
            .apply("RandomColumnarSample", RandomColumnarSampler.any(options.getSampleSize()))
            .apply("BatchForDlp", new BatchColumnsForDlp())
            .apply(
                "DlpIdentify", DlpIdentify.withIdentifierFactory(makeDlpBatchIdentifierFactory()));

    // Write Column Information to GCS file.
    columnInformation.apply(
        "WriteColumnReport",
        FileIO.<String, ColumnInformation>writeDynamic()
            .via(
                Contextful.fn(JsonConvertor::asJsonString), Contextful.fn(colName -> TextIO.sink()))
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

    // Write Column Information to Data Catalog
    if (isNotBlank(options.getDataCatalogInspectionTagTemplateId())) {

      columnInformation
          .apply(
              "ExtractDataCatalogItems",
              ExtractDataCatalogItems.builder()
                  .setSchema(recordSchemaTuple.get(avroSchemaTag).apply(View.asSingleton()))
                  .setSourceType(options.getSourceType())
                  .setInputPattern(options.getInputPattern())
                  .setInspectionTagTemplateId(options.getDataCatalogInspectionTagTemplateId())
                  .setJdbcConfiguration(extractJdbcConfiguration())
                  .build())
          .apply(
              "WriteItemsToCatalog",
              DataCatalogWriter.builder()
                  .setEntryGroupId(options.getDataCatalogEntryGroupId())
                  .setInspectionTagTemplateId(options.getDataCatalogInspectionTagTemplateId())
                  .setForceUpdate(options.getDataCatalogForcedUpdate())
                  .build());
    }

    return pipeline;
  }

  private void validateOptions() {

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
  private DlpSenderFactory makeDlpBatchIdentifierFactory() {
    var dlpIdentifierBuilder =
        DlpSenderFactory.builder()
            .projectId(options.getProject())
            .dlpFactory(DlpClientFactory.defaultFactory());

    if (options.getObservableInfoTypes() != null && !options.getObservableInfoTypes().isEmpty()) {
      ImmutableSet<InfoType> userProvidedInfoTypes =
          options.getObservableInfoTypes().stream()
              .map(InfoType.newBuilder()::setName)
              .map(InfoType.Builder::build)
              .collect(toImmutableSet());

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
}
