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

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

import com.google.api.client.util.Maps;
import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.common.DeidentifyColumns;
import com.google.cloud.solutions.autotokenize.common.ZippingIterator;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.flogger.GoogleLogger;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Provides batching of column-values into DLP table, Along with a map of ColumnName mapping.
 */
public class DlpColumnValueAccumulator implements
    BatchAccumulator<KV<String, Iterable<Value>>, KV<Table, Map<String, String>>> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final int MAX_SERIALIZED_SIZE = 500000; // 500KB = max payload size for Dlp Content API
  private static final int MAX_TABLE_ROWS_COUNT = 50000; // 50K
  private final HashMap<String, LinkedList<Value>> values = Maps.newHashMap();
  private int accumulatedSize = 0;
  private int accumulatedElements = 0;

  public static DlpColumnValueAccumulator create() {
    return new DlpColumnValueAccumulator();
  }

  @Override
  public boolean addElement(KV<String, Iterable<Value>> newColumnValues) {
    List<Value> newValues = Lists.newLinkedList(newColumnValues.getValue());
    int newSize =
        computeSerializedSize(newValues) + FieldId.newBuilder().setName(newColumnValues.getKey())
            .build().getSerializedSize();
    int newElementsCount = newValues.size() + 1; //+1 for header.

    if (accumulatedSize + newSize <= MAX_SERIALIZED_SIZE
        && accumulatedElements + newElementsCount <= MAX_TABLE_ROWS_COUNT) {
      if (!values.containsKey(newColumnValues.getKey())) {
        values.put(newColumnValues.getKey(), Lists.newLinkedList());
      }
      //Append values to header values if headerName exists.
      values.get(newColumnValues.getKey()).addAll(newValues);

      logger.atInfo().log("Adding Col:%s, Elements:%s", newColumnValues.getKey(), newValues.size());

      accumulatedSize += newSize;
      accumulatedElements += newElementsCount;

      return true;
    }

    return false;
  }

  @Override
  public Batch<KV<Table, Map<String, String>>> makeBatch() {
    List<String> allHeaders = ImmutableList.copyOf(values.keySet());

    // Ensure same sequence of value list as headers.
    List<List<Value>> allValues = allHeaders.stream().map(values::get).collect(toList());

    List<Table.Row> valueRows =
        ZippingIterator.createElementStream(allValues)
            .map(ConvertEmptyElementsToDefault.create())
            .map(e -> Table.Row.newBuilder().addAllValues(e).build())
            .collect(toList());

    Table batchedTable =
        Table.newBuilder()
            .addAllHeaders(DeidentifyColumns.fieldIdsFor(allHeaders))
            .addAllRows(valueRows)
            .build();

    Map<String, String> columnNameMap = allHeaders.stream()
        .collect(toImmutableMap(identity(), identity()));

    return DlpTableBatch.create(KV.of(batchedTable, columnNameMap));
  }

  /**
   * Returns the total serialized size of all the elements in the list.
   */
  private static int computeSerializedSize(List<Value> values) {
    if (values == null) {
      return 0;
    }

    return values.stream().mapToInt(Value::getSerializedSize).sum();
  }

  /**
   * Converts empty elements as {@link Value#getDefaultInstance()}
   */
  private static class ConvertEmptyElementsToDefault implements
      Function<List<Optional<Value>>, List<Value>> {

    static ConvertEmptyElementsToDefault create() {
      return new ConvertEmptyElementsToDefault();
    }

    @Override
    public @Nullable List<Value> apply(@Nullable List<Optional<Value>> elements) {
      return elements.stream().map(element -> element.orElse(Value.getDefaultInstance()))
          .collect(toList());
    }
  }


  /**
   * Represents Batch of elements as {@link Table} elements and its attributes.
   */
  @AutoValue
  public static abstract class DlpTableBatch implements
    BatchAccumulator.Batch<KV<Table, Map<String, String>>> {

    /**
     * Returns number of columns in the table.
     */
    abstract int columns();

    /**
     * Returns number of rows excluding header row in the table.
     */
    abstract int rows();

    @Override
    public final String report() {
      return MoreObjects.toStringHelper(DlpTableBatch.class)
        .add("columns", columns())
        .add("rows", rows())
        .add("serializedSize", serializedSize())
        .toString();
    }

    public static DlpTableBatch create(KV<Table, Map<String, String>> batchValue) {
      int rows = firstNonNull(batchValue.getKey().getRowsCount(), 0);
      int columns = firstNonNull(batchValue.getKey().getHeadersCount(), 0);

      return new AutoValue_DlpColumnValueAccumulator_DlpTableBatch(batchValue, rows * columns,
        batchValue.getKey().getSerializedSize(), columns, rows);
    }
  }
}
