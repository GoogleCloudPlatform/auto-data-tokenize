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

import static com.google.cloud.solutions.autotokenize.common.CsvRowFlatRecordConvertors.flatRecordToCsvRowFn;
import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.common.CsvIO;
import com.google.cloud.solutions.autotokenize.common.CsvIO.CsvRow;
import com.google.cloud.solutions.autotokenize.common.SecretsClient;
import com.google.cloud.solutions.autotokenize.common.SortCsvRow;
import com.google.cloud.solutions.autotokenize.dlp.DlpClientFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Deploys a pipeline to read CSV files, tokenize using DLP and output as CSV with optional sorting
 * or rows.
 */
public final class CsvTokenizationAndOrderingPipeline extends EncryptionPipeline {

  public interface CsvTokenizationAndOrderingPipelineOptions extends EncryptionPipelineOptions {

    @Default.Integer(5)
    int getCsvFileShardCount();

    void setCsvFileShardCount(int csvFileShards);

    /* Provide one-of groupingColumns or groupingColumnNames */

    List<Integer> getOrderingColumns();

    void setOrderingColumns(List<Integer> orderingColumnName);

    List<String> getOrderingColumnNames();

    void setOrderingColumnNames(List<String> orderingColumnName);
  }

  public static void main(String[] args) throws Exception {
    PipelineOptionsFactory.register(CsvTokenizationAndOrderingPipelineOptions.class);

    var options =
        PipelineOptionsFactory.fromArgs(args).as(CsvTokenizationAndOrderingPipelineOptions.class);
    checkArgument(isNotBlank(options.getOutputDirectory()), "Provide a valid GCS Destination");

    new CsvTokenizationAndOrderingPipeline(
            options,
            Pipeline.create(options),
            DlpClientFactory.defaultFactory(),
            SecretsClient.of(),
            KeyManagementServiceClient.create())
        .run();
  }

  private final CsvTokenizationAndOrderingPipelineOptions options;

  @VisibleForTesting
  CsvTokenizationAndOrderingPipeline(
      CsvTokenizationAndOrderingPipelineOptions options,
      Pipeline pipeline,
      DlpClientFactory dlpClientFactory,
      SecretsClient secretsClient,
      KeyManagementServiceClient kmsClient) {
    super(makeCsvOptions(options), pipeline, dlpClientFactory, secretsClient, kmsClient);
    this.options = options;
  }

  private static CsvTokenizationAndOrderingPipelineOptions makeCsvOptions(
      CsvTokenizationAndOrderingPipelineOptions options) {
    options.setSourceType(SourceType.CSV_FILE);
    return options;
  }

  @Override
  public PipelineResult run() throws Exception {
    applyReadAndEncryptionSteps()
        .apply("MakeCsvRecord", MapElements.via(flatRecordToCsvRowFn()))
        .apply(
            "SortCsvRows",
            (options.getOrderingColumns() != null || options.getOrderingColumnNames() != null)
                ? csvSorter()
                : MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptor.of(String.class),
                            TypeDescriptors.iterables(TypeDescriptor.of(CsvRow.class))))
                    .via(
                        (SerializableFunction<CsvRow, KV<String, Iterable<CsvRow>>>)
                            input -> KV.of("", List.of(input))))
        .apply(
            "WriteCsv",
            CsvIO.write(new SortedRowIterableFn())
                .withOutputFilePrefix(options.getOutputDirectory())
                .withFileShards(options.getCsvFileShardCount()));

    return pipeline.run();
  }

  private SortCsvRow csvSorter() {
    var csvRowSorterBuilder = SortCsvRow.builder().setTempLocation(options.getTempLocation());

    if (options.getOrderingColumns() != null) {
      csvRowSorterBuilder.setOrderingColumns(ImmutableList.copyOf(options.getOrderingColumns()));
    } else {
      csvRowSorterBuilder.setOrderingColumnNames(
          ImmutableList.copyOf(options.getOrderingColumnNames()));
    }

    return csvRowSorterBuilder.build();
  }

  private static final class SortedRowIterableFn
      extends SimpleFunction<KV<String, Iterable<CsvRow>>, Iterable<CsvRow>> {

    @Override
    public Iterable<CsvRow> apply(KV<String, Iterable<CsvRow>> input) {
      return input.getValue();
    }
  }
}
