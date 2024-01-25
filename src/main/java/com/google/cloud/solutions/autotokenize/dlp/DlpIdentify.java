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

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.InfoTypeInformation;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.Table;
import java.io.IOException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Composite transform to send batched data to DLP and extract {@link InfoType} information for each
 * of the input columns.
 *
 * <p>The expected {@code KV<Table, Map<String,String>>} is a pair of DLP table and a Map of headers
 * used in the table to reporting columnNames.
 *
 * <pre>{@code
 * PCollection<KV<Table, Map<String, String>>> batchedData = ...;
 *
 * batchedData
 *   .apply(DlpIdentify.withIdentifierFactory(dlpSenderFactory);
 * }</pre>
 */
@AutoValue
public abstract class DlpIdentify
    extends PTransform<PCollection<KV<ShardedKey<String>, Table>>, PCollectionTuple> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  abstract DlpBatchInspectFactory batchIdentifierFactory();

  abstract TupleTag<ColumnInformation> columnInfoTag();

  abstract TupleTag<KV<ShardedKey<String>, Table>> errorTag();

  public static Builder builder() {
    return new AutoValue_DlpIdentify.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder batchIdentifierFactory(DlpBatchInspectFactory batchIdentifierFactory);

    public abstract Builder columnInfoTag(TupleTag<ColumnInformation> columnInfoTag);

    public abstract Builder errorTag(TupleTag<KV<ShardedKey<String>, Table>> errorTag);

    public abstract DlpIdentify build();
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<ShardedKey<String>, Table>> batchedRecords) {

    var coderRegistry = batchedRecords.getPipeline().getCoderRegistry();

    coderRegistry.registerCoderForType(
        TypeDescriptors.kvs(
            TypeDescriptor.of(Table.class),
            TypeDescriptors.maps(TypeDescriptor.of(String.class), TypeDescriptor.of(String.class))),
        KvCoder.of(
            ProtoCoder.of(Table.class), MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

    coderRegistry.registerCoderForType(
        TypeDescriptors.kvs(TypeDescriptor.of(String.class), TypeDescriptor.of(InfoType.class)),
        KvCoder.of(StringUtf8Coder.of(), ProtoCoder.of(InfoType.class)));

    TupleTag<KV<String, InfoType>> successElements = new TupleTag<>();

    var successAndErrorElements =
        batchedRecords.apply(
            "IdentifyUsingDLP",
            ParDo.of(SendToDlp.create(batchIdentifierFactory(), errorTag()))
                .withOutputTags(successElements, TupleTagList.of(errorTag())));

    var columnInformation =
        successAndErrorElements
            .get(successElements)
            .apply("CountInfoTypesPerColumn", Count.perElement())
            .apply("ParseCounts", MapElements.via(new CountFlattener()))
            .apply("GroupByColumns", GroupByKey.create())
            .apply(
                "CreateColumnInformation",
                MapElements.into(TypeDescriptor.of(ColumnInformation.class))
                    .via(
                        columnGroupKv ->
                            ColumnInformation.newBuilder()
                                .setColumnName(columnGroupKv.getKey())
                                .addAllInfoTypes(columnGroupKv.getValue())
                                .build()));

    return PCollectionTuple.of(columnInfoTag(), columnInformation)
        .and(errorTag(), successAndErrorElements.get(errorTag()));
  }

  /** Sends the batched records table to DLP identify service and emits the response. */
  @AutoValue
  abstract static class SendToDlp
      extends DoFn<KV<ShardedKey<String>, Table>, KV<String, InfoType>> {
    private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

    abstract DlpBatchInspectFactory identifierFactory();

    @Nullable
    abstract TupleTag<KV<ShardedKey<String>, Table>> errorTag();

    private DlpBatchInspect dlpBatchInspect;

    public static SendToDlp create(
        DlpBatchInspectFactory identifierFactory,
        TupleTag<KV<ShardedKey<String>, Table>> errorTag) {
      return new AutoValue_DlpIdentify_SendToDlp(identifierFactory, errorTag);
    }

    @Setup
    public void createDlpClient() throws IOException {
      dlpBatchInspect = identifierFactory().newSender();
    }

    @Teardown
    public void shutdownDlpClient() {
      if (dlpBatchInspect != null) {
        dlpBatchInspect.shutdownClient();
      }
    }

    @ProcessElement
    public void inspectTable(
        @Element KV<ShardedKey<String>, Table> dlpTableForIdentify, ProcessContext processContext) {
      var columnName = dlpTableForIdentify.getKey().getKey();
      var dlpTable = dlpTableForIdentify.getValue();

      try {
        dlpBatchInspect
            .identifyInfoTypes(KV.of(dlpTable, ImmutableMap.of(columnName, columnName)))
            .forEach(processContext::output);
      } catch (RuntimeException exception) {
        logger.atSevere().withCause(exception).log(
            "Error processing batch: key: %s, bytes: %s, elementCount: %s",
            columnName, dlpTable.getSerializedSize(), dlpTable.getRowsCount());

        if (errorTag() != null) {
          processContext.output(errorTag(), dlpTableForIdentify);
        }
      }
    }
  }

  /** Extracts column name as Key and rest of the InfoType and count information as value. */
  private static class CountFlattener
      extends SimpleFunction<KV<KV<String, InfoType>, Long>, KV<String, InfoTypeInformation>> {

    @Override
    public KV<String, InfoTypeInformation> apply(
        KV<KV<String, InfoType>, Long> columnInfoTypeCounts) {
      return KV.of(
          columnInfoTypeCounts.getKey().getKey(),
          InfoTypeInformation.newBuilder()
              .setInfoType(columnInfoTypeCounts.getKey().getValue().getName())
              .setCount(columnInfoTypeCounts.getValue())
              .build());
    }
  }
}
