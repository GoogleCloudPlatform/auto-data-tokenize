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

package com.google.cloud.solutions.autotokenize.pipeline.dlp;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.InfoTypeInformation;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.Table;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Composite transform to send batched data to DLP and extract {@link InfoType} information for each
 * of the input columns.
 *
 * <p> The expected {@code KV<Table, Map<String,String>>} is a pair of DLP table and a Map of
 * headers used in the table to reporting columnNames.
 *
 * <pre>{@code
 *   PCollection<KV<Table, Map<String, String>>> batchedData = ...;
 *
 *   batchedData
 *     .apply(DlpIdentify.withIdentifierFactory(dlpSenderFactory);
 * }</pre>
 */
@AutoValue
public abstract class DlpIdentify extends
    PTransform<PCollection<KV<Table, Map<String, String>>>, PCollection<ColumnInformation>> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  abstract DlpSenderFactory batchIdentifierFactory();

  public static DlpIdentify withIdentifierFactory(DlpSenderFactory batchIdentifierFactory) {
    return new AutoValue_DlpIdentify(batchIdentifierFactory);
  }

  @Override
  public PCollection<ColumnInformation> expand(
      PCollection<KV<Table, Map<String, String>>> batchedRecords) {
    return batchedRecords
        .apply("IdentifyUsingDLP", MapElements.via(SendToDlp.create(batchIdentifierFactory())))
        .apply("FlattenColumnFindings", Flatten.iterables())
        .apply("CountInfoTypesPerColumn", Count.perElement())
        .apply("ParseCounts", MapElements.via(new CountFlattener()))
        .apply("GroupByColumns", GroupByKey.create())
        .apply("CreateColumnInformation",
            MapElements
                .into(TypeDescriptor.of(ColumnInformation.class))
                .via(
                    columnGroupKv ->
                        ColumnInformation.newBuilder()
                            .setColumnName(columnGroupKv.getKey())
                            .addAllInfoTypes(columnGroupKv.getValue())
                            .build()));
  }

  /**
   * Sends the batched records table to DLP identify service and emits the response.
   */
  @AutoValue
  static abstract class SendToDlp extends
      SimpleFunction<KV<Table, Map<String, String>>, List<KV<String, InfoType>>> {

    abstract DlpSenderFactory identifierFactory();

    public static SendToDlp create(DlpSenderFactory identifierFactory) {
      return new AutoValue_DlpIdentify_SendToDlp(identifierFactory);
    }

    @Override
    public List<KV<String, InfoType>> apply(KV<Table, Map<String, String>> dlpTableForIdentify) {

      try {
        return identifierFactory().newSender().identifyInfoTypes(dlpTableForIdentify);
      } catch (IOException ioException) {
        logger.atSevere().withCause(ioException).log("unable to send the batch.");
      }
      return ImmutableList.of();
    }
  }

  /**
   * Extracts column name as Key and rest of the InfoType and count information as value.
   */
  private static class CountFlattener extends
      SimpleFunction<KV<KV<String, InfoType>, Long>, KV<String, InfoTypeInformation>> {

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
