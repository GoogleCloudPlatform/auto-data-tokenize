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

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.beam.sdk.io.FileIO.Write.defaultNaming;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.common.SourceNames;
import com.google.cloud.solutions.autotokenize.common.TransformingFileReader;
import com.google.cloud.solutions.autotokenize.common.util.JsonConvertor;
import com.google.cloud.solutions.autotokenize.pipeline.dlp.BatchColumnsForDlp;
import com.google.cloud.solutions.autotokenize.pipeline.dlp.DlpClientFactory;
import com.google.cloud.solutions.autotokenize.pipeline.dlp.DlpIdentify;
import com.google.cloud.solutions.autotokenize.pipeline.dlp.DlpSenderFactory;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.privacy.dlp.v2.InfoType;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Instantiates the sampling Dataflow pipeline based on provided options.
 */
public final class DlpSamplerIdentifyPipeline {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final DlpSamplerIdentifyOptions options;

  private final Pipeline pipeline;

  private DlpSamplerIdentifyPipeline(DlpSamplerIdentifyOptions options) {
    this.options = options;
    this.pipeline = Pipeline.create(options);
  }

  /**
   * Creates the pipeline and applies the transforms.
   */
  private Pipeline makePipeline() {
    TupleTag<FlatRecord> recordsTag = new TupleTag<>();
    TupleTag<String> avroSchemaTag = new TupleTag<>();

    PCollectionTuple recordSchemaTuple =
      pipeline
        .apply("Read" + SourceNames.forType(options.getSourceType()).asCamelCase(),
          TransformingFileReader
            .forSourceType(options.getSourceType())
            .from(options.getInputPattern())
            .withRecordsTag(recordsTag)
            .withAvroSchemaTag(avroSchemaTag));

    // Write the schema to a file.
    recordSchemaTuple.get(avroSchemaTag)
      .apply("WriteSchema",
        TextIO.write().to(options.getReportLocation()).withoutSharding()
          .withSuffix("schema.json"));

    // Sample and Identify columns
    recordSchemaTuple.get(recordsTag)
      .apply("RandomColumnarSample", RandomColumnarSampler.any(options.getSampleSize()))
      .apply("BatchForDlp", new BatchColumnsForDlp())
      .apply("DlpIdentify", DlpIdentify.withIdentifierFactory(makeDlpBatchIdentifierFactory()))
      .apply("WriteColumnReport",
        FileIO.<String, ColumnInformation>writeDynamic()
          .via(Contextful.fn(JsonConvertor::asJsonString),
            Contextful.fn(colName -> TextIO.sink()))
          .by(ColumnInformation::getColumnName)
          .withDestinationCoder(StringUtf8Coder.of())
          .withNoSpilling()
          .withNaming(
            Contextful.fn(
              colName ->
                defaultNaming( /*prefix=*/
                  String.format("col-%s", colName.replaceAll("[\\.\\$\\[\\]]+", "-"))
                    .replaceAll("[-]+", "-"), /*suffix=*/ ".json")))

          .to(options.getReportLocation()));

    return pipeline;
  }

  /**
   * Returns a factory to create {@link DlpServiceClient} instance using {@code
   * DlpServiceClient.create()} method.
   */
  private DlpSenderFactory makeDlpBatchIdentifierFactory() {
    DlpSenderFactory.Builder dlpIdentifierBuilder =
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
    DlpSamplerIdentifyOptions options = PipelineOptionsFactory.fromArgs(args)
      .as(DlpSamplerIdentifyOptions.class);

    logger.atInfo().log("Staging the Dataflow job");

    DataflowPipelineJob samplingJob =
      (DataflowPipelineJob) new DlpSamplerIdentifyPipeline(options).makePipeline().run();

    logger.atInfo().log(
      "JobLink: https://console.cloud.google.com/dataflow/jobs/%s?project=%s%n",
      samplingJob.getJobId(),
      samplingJob.getProjectId());
  }
}
