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


import com.google.common.flogger.GoogleLogger;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Table.Row;
import com.google.privacy.dlp.v2.Value;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** Splits the columnName-values lists into chunks that are within the DLP Table limits. */
public class BatchColumnsForDlp
    extends PTransform<PCollection<KV<String, Value>>, PCollection<KV<ShardedKey<String>, Table>>> {

  private static final int DLP_BATCH_SIZE = 480000;
  private static final int DLP_MAX_BATCH_PAYLOAD_SIZE = 500000;
  private static final int DLP_MAX_ELEMENTS_COUNT = 50000;

  @Override
  public PCollection<KV<ShardedKey<String>, Table>> expand(PCollection<KV<String, Value>> input) {

    return input
        .apply("FilterNormalElements", Filter.by(new ValidValueChecker()))
        .apply(
            "GroupElements",
            GroupIntoBatches.<String, Value>ofByteSize(
                    DLP_BATCH_SIZE, (Value v) -> (long) v.getSerializedSize())
                .withShardedKey())
        .apply("MakeDlpTable", ParDo.of(new ColumnTableMakerFn()))
        .setCoder(
            KvCoder.of(ShardedKey.Coder.of(StringUtf8Coder.of()), ProtoCoder.of(Table.class)));
    //        .apply(Reshuffle.<KV<ShardedKey<String>, Table>>viaRandomKey().withNumBuckets());
  }

  /** Batching of column-values into DLP table, Along with a map of ColumnName mapping. */
  private static class ColumnTableMakerFn
      extends DoFn<KV<ShardedKey<String>, Iterable<Value>>, KV<ShardedKey<String>, Table>> {

    private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

    @ProcessElement
    public void makeTables(
        @Element KV<ShardedKey<String>, Iterable<Value>> input,
        OutputReceiver<KV<ShardedKey<String>, Table>> outputReceiver) {

      if (input == null || input.getKey() == null || input.getValue() == null) {
        return;
      }

      var columnName = input.getKey();

      var accumulatedTable = new ColumnTable(columnName);

      for (var value : input.getValue()) {

        if (accumulatedTable.add(value)) {
          continue;
        }

        emitTable(input.getKey(), accumulatedTable.getTable(), outputReceiver);
        accumulatedTable = new ColumnTable(columnName);
        accumulatedTable.add(value);
      }

      if (accumulatedTable.getTable().getRowsCount() > 0) {
        emitTable(input.getKey(), accumulatedTable.getTable(), outputReceiver);
      }
    }

    private void emitTable(
        ShardedKey<String> columnName,
        Table emitTable,
        OutputReceiver<KV<ShardedKey<String>, Table>> outputReceiver) {
      logger.atInfo().log(
          "Emitting table| columnName: %s | bytes: %s | elements: %s",
          columnName.getKey(),
          emitTable.getSerializedSize(),
          emitTable.getRowsCount() + emitTable.getHeadersCount());
      outputReceiver.output(KV.of(columnName, emitTable));
    }

    /** Decorator class for accumulating column Values. */
    private static class ColumnTable {
      private final ShardedKey<String> columnName;

      private Table accumulatedTable;
      private int accumulatedElementsCount;

      public ColumnTable(ShardedKey<String> columnName) {
        this.columnName = columnName;
        this.accumulatedTable =
            Table.newBuilder()
                .addHeaders(FieldId.newBuilder().setName(columnName.getKey()))
                .build();
        this.accumulatedElementsCount = 1;
      }

      public boolean add(Value value) {

        var tempTable = accumulatedTable.toBuilder().addRows(makeValueRow(value)).build();

        if (tempTable.getSerializedSize() > DLP_MAX_BATCH_PAYLOAD_SIZE
            || (accumulatedElementsCount + 1) >= DLP_MAX_ELEMENTS_COUNT) {
          return false;
        }

        accumulatedTable = tempTable;
        accumulatedElementsCount++;

        return true;
      }

      private static Row makeValueRow(Value value) {
        return Row.newBuilder().addValues(value).build();
      }

      public Table getTable() {
        return accumulatedTable;
      }
    }
  }

  /**
   * Checks if a specific table cell-value fits into the max DLP Batch size for filtering out
   * excessively large elements.
   */
  private static class ValidValueChecker implements ProcessFunction<KV<String, Value>, Boolean> {
    @Override
    public Boolean apply(KV<String, Value> element) {
      return element != null
          && element.getKey() != null
          && element.getValue() != null
          && element.getValue().getSerializedSize() <= DLP_BATCH_SIZE;
    }
  }
}
