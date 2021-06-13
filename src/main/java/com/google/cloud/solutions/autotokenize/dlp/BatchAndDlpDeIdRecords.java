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

package com.google.cloud.solutions.autotokenize.dlp;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.auto.value.AutoValue;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.PartialColumnDlpTable;
import com.google.cloud.solutions.autotokenize.common.DeidentifyColumns;
import com.google.cloud.solutions.autotokenize.common.TokenizeColumnNameUpdater;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.common.flogger.GoogleLogger;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoValue
public abstract class BatchAndDlpDeIdRecords
    extends PTransform<PCollection<FlatRecord>, PCollection<FlatRecord>> {

  public static final int DEFAULT_SHARDS_COUNT = 5;

  abstract int shardCount();

  abstract DlpEncryptConfig encryptConfig();

  @Nullable
  abstract String dlpProjectId();

  @Nullable
  abstract DlpClientFactory dlpClientFactory();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder shardCount(int shardCount);

    public abstract Builder dlpProjectId(String dlpProjectId);

    public abstract Builder encryptConfig(DlpEncryptConfig encryptConfig);

    public abstract Builder dlpClientFactory(DlpClientFactory dlpClientFactory);

    public abstract BatchAndDlpDeIdRecords build();
  }

  abstract Builder toBuilder();

  public static BatchAndDlpDeIdRecords withEncryptConfig(DlpEncryptConfig dlpEncryptConfig) {
    return new AutoValue_BatchAndDlpDeIdRecords.Builder()
        .shardCount(DEFAULT_SHARDS_COUNT)
        .encryptConfig(dlpEncryptConfig)
        .build();
  }

  public BatchAndDlpDeIdRecords withShardsCount(int shardsCount) {
    return toBuilder().shardCount(shardsCount).build();
  }

  public BatchAndDlpDeIdRecords withDlpClientFactory(DlpClientFactory clientFactory) {
    return toBuilder().dlpClientFactory(clientFactory).build();
  }

  public BatchAndDlpDeIdRecords withDlpProjectId(String dlpProjectId) {
    return toBuilder().dlpProjectId(dlpProjectId).build();
  }

  @Override
  public PCollection<FlatRecord> expand(PCollection<FlatRecord> input) {
    checkNotNull(dlpClientFactory(), "Provide Dlp client factory");
    checkArgument(isNotBlank(dlpProjectId()), "DLP ProjectId can't be empty.");

    return input
        .apply("AddRecordId", MapElements.via(FlatRecordKeysFn.create()))
        .apply(MapElements.via(new ShardAssigner<>(shardCount())))
        .apply(
            "BatchForDlp", GroupByBatchSize.with(PartialBatchAccumulator.factory(encryptConfig())))
        .setCoder(ProtoCoder.of(PartialColumnDlpTable.class))
        .apply(
            ParDo.of(
                DlpDeidentifyFn.builder()
                    .encryptConfig(encryptConfig())
                    .dlpClientFactory(dlpClientFactory())
                    .dlpProjectId(dlpProjectId())
                    .build()))
        .apply(Flatten.iterables());
  }

  private static class ShardAssigner<T> extends SimpleFunction<T, KV<String, T>> {

    private final int maxShards;

    public ShardAssigner(int maxShards) {
      this.maxShards = maxShards;
    }

    @Override
    public KV<String, T> apply(T input) {
      return KV.of(String.valueOf(input.hashCode() % maxShards), input);
    }
  }

  private static class FlatRecordKeysFn extends SimpleFunction<FlatRecord, FlatRecord> {

    public static FlatRecordKeysFn create() {
      return new FlatRecordKeysFn();
    }

    @Override
    public FlatRecord apply(FlatRecord input) {

      String recordId = input.getRecordId();
      if (isBlank(recordId)) {
        recordId = UUID.randomUUID().toString();
      }

      return input.toBuilder().setRecordId(recordId).build();
    }
  }

  @AutoValue
  abstract static class DlpDeidentifyFn extends DoFn<PartialColumnDlpTable, Iterable<FlatRecord>> {

    abstract String dlpProjectId();

    abstract DlpClientFactory dlpClientFactory();

    abstract DlpEncryptConfig encryptConfig();

    private transient DlpServiceClient dlpClient;

    @Setup
    public void buildDlpClient() throws IOException {
      dlpClient = dlpClientFactory().newClient();
    }

    @Teardown
    public void shutDownClient() {
      dlpClient.close();
    }

    @ProcessElement
    public void processBatch(
        @Element PartialColumnDlpTable batch, OutputReceiver<Iterable<FlatRecord>> receiver) {

      var batchTable = batch.getTable();

      GoogleLogger.forEnclosingClass()
          .atFine()
          .log(
              "Sending Batch:%nBytes:%s%nColumns:%s%nRowCount:%s",
              batchTable.getSerializedSize(),
              DeidentifyColumns.columnNamesFromHeaders(batchTable.getHeadersList()),
              batchTable.getRowsCount());

      DeidentifyContentResponse deidResponse =
          dlpClient.deidentifyContent(
              DeidentifyContentRequest.newBuilder()
                  .setParent(String.format("projects/%s", dlpProjectId()))
                  .setDeidentifyConfig(batch.getDeidentifyConfig())
                  .setItem(ContentItem.newBuilder().setTable(batchTable).build())
                  .build());

      receiver.output(
          TokenizedDataMerger.create(batch, deidResponse.getItem().getTable(), encryptConfig())
              .merge());
    }

    public static Builder builder() {
      return new AutoValue_BatchAndDlpDeIdRecords_DlpDeidentifyFn.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder dlpProjectId(String dlpProjectId);

      public abstract Builder dlpClientFactory(DlpClientFactory dlpClientFactory);

      public abstract Builder encryptConfig(DlpEncryptConfig encryptConfig);

      public abstract DlpDeidentifyFn build();
    }
  }

  @AutoValue
  abstract static class TokenizedDataMerger {

    abstract PartialColumnDlpTable batch();

    abstract Table deidTable();

    abstract DlpEncryptConfig encryptConfig();

    public static TokenizedDataMerger create(
        PartialColumnDlpTable batch, Table deidTable, DlpEncryptConfig encryptConfig) {
      return new AutoValue_BatchAndDlpDeIdRecords_TokenizedDataMerger(
          batch, deidTable, encryptConfig);
    }

    ImmutableList<FlatRecord> merge() {

      var columnNameUpdater =
          new TokenizeColumnNameUpdater(DeidentifyColumns.columnNamesIn(encryptConfig()));

      ImmutableList<String> headers = DeidentifyColumns.columnNamesIn(deidTable());

      ImmutableMap<String, ImmutableMap<String, Value>> deidValues =
          deidTable().getRowsList().stream()
              .map(
                  row -> {
                    @SuppressWarnings("UnstableApiUsage")
                    ImmutableMap<String, Value> valueMap =
                        Streams.zip(
                                headers.stream(), row.getValuesList().stream(), ImmutablePair::of)
                            .collect(
                                toImmutableMap(ImmutablePair::getLeft, ImmutablePair::getRight));

                    var recordId = valueMap.get(batch().getRecordIdColumnName()).getStringValue();

                    ImmutableMap<String, Value> valuesMapWithoutRecordId =
                        valueMap.entrySet().stream()
                            .filter(e -> !e.getKey().equals(batch().getRecordIdColumnName()))
                            .filter(e -> !e.getValue().equals(Value.getDefaultInstance()))
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

                    return ImmutablePair.of(recordId, valuesMapWithoutRecordId);
                  })
              .collect(toImmutableMap(ImmutablePair::getLeft, ImmutablePair::getRight));

      return batch().getRecordsList().stream()
          .map(
              genericRecord -> {
                var flatRecordUpdatedValues =
                    genericRecord.toBuilder()
                        .putAllValues(deidValues.get(genericRecord.getRecordId()))
                        .build();

                return columnNameUpdater.updateColumnNames(flatRecordUpdatedValues);
              })
          .collect(toImmutableList());
    }
  }
}
