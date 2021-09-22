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

package com.google.cloud.solutions.autotokenize.common;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Boolean.logicalXor;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.common.CsvIO.CsvRow;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import java.util.List;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.ExternalSorter.Options.SorterType;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoValue
public abstract class SortCsvRow
    extends PTransform<PCollection<CsvRow>, PCollection<KV<String, Iterable<CsvRow>>>> {

  @Nullable
  abstract ImmutableList<Integer> getOrderingColumns();

  @Nullable
  abstract ImmutableList<String> getOrderingColumnNames();

  abstract String getTempLocation();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setOrderingColumns(ImmutableList<Integer> newOrderingColumns);

    public abstract Builder setOrderingColumnNames(ImmutableList<String> newOrderingColumnNames);

    public abstract Builder setTempLocation(String newTempLocation);

    abstract SortCsvRow autoBuild();

    public SortCsvRow build() {
      var transform = autoBuild();

      checkArgument(
          isNullOrSizeValid(transform.getOrderingColumns()),
          "provide columns for sorting, max of 2");
      checkArgument(
          isNullOrSizeValid(transform.getOrderingColumnNames()),
          "provide columnNames for sorting, max of 2");
      checkArgument(
          logicalXor(
              transform.getOrderingColumnNames() != null, transform.getOrderingColumns() != null),
          "Provide exactly only one of orderingColumns or orderingColumnNames");

      return transform;
    }

    private static <T> boolean isNullOrSizeValid(List<T> list) {
      return (list == null) || (list.size() > 0 && list.size() <= 2);
    }
  }

  @Override
  public PCollection<KV<String, Iterable<CsvRow>>> expand(PCollection<CsvRow> input) {

    return input
        .apply("ConvertToKV", MapElements.via(kvConverter()))
        .apply("GroupUsingOrderingFields", GroupByKey.create())
        .apply(
            "Sort",
            SortValues.create(
                BufferedExternalSorter.options()
                    .withExternalSorterType(SorterType.NATIVE)
                    .withTempLocation(getTempLocation())))
        .apply("ExtractSortedGroupedElements", MapElements.via(ExtractSortedGroupedElements.of()));
  }

  private SimpleFunction<CsvRow, KV<String, KV<String, CsvRow>>> kvConverter() {

    var orderingIndexPresent = getOrderingColumns() != null && !getOrderingColumns().isEmpty();

    return (orderingIndexPresent)
        ? CsvRowToSortIndexKV.usingColumns(getOrderingColumns())
        : CsvRowToSortNameKV.usingColumnNames(getOrderingColumnNames());
  }

  public static Builder builder() {
    return new AutoValue_SortCsvRow.Builder();
  }

  private static final String DEFAULT_SORT_KEY_VALUE = "";

  private static final class ExtractSortedGroupedElements
      extends SimpleFunction<
          KV<String, Iterable<KV<String, CsvRow>>>, KV<String, Iterable<CsvRow>>> {

    static ExtractSortedGroupedElements of() {
      return new ExtractSortedGroupedElements();
    }

    @Override
    public KV<String, Iterable<CsvRow>> apply(KV<String, Iterable<KV<String, CsvRow>>> input) {
      var extractedOrderedRows =
          ImmutableList.copyOf(Iterators.transform(input.getValue().iterator(), KV::getValue));

      return KV.of(input.getKey(), extractedOrderedRows);
    }
  }

  /** */
  @AutoValue
  abstract static class CsvRowToSortIndexKV
      extends SimpleFunction<CsvRow, KV<String, KV<String, CsvRow>>> {

    abstract Integer primaryOrderKey();

    @Nullable
    abstract Integer secondaryOrderKey();

    @Override
    public KV<String, KV<String, CsvRow>> apply(CsvRow input) {
      var key1 = input.get(primaryOrderKey());
      var key2 =
          (secondaryOrderKey() != null) ? input.get(secondaryOrderKey()) : DEFAULT_SORT_KEY_VALUE;

      return KV.of(key1, KV.of(key2, input));
    }

    public static CsvRowToSortIndexKV create(
        Integer primaryOrderKey, @Nullable Integer secondaryOrderKey) {
      return new AutoValue_SortCsvRow_CsvRowToSortIndexKV(primaryOrderKey, secondaryOrderKey);
    }

    public static CsvRowToSortIndexKV usingColumns(List<Integer> orderingColumns) {
      checkNotNull(orderingColumns);
      checkArgument(orderingColumns.size() > 0 && orderingColumns.size() <= 2);

      var primaryKey = orderingColumns.get(0);
      var secondaryKey = (orderingColumns.size() == 2) ? orderingColumns.get(1) : null;

      return create(primaryKey, secondaryKey);
    }
  }

  /** */
  @AutoValue
  abstract static class CsvRowToSortNameKV
      extends SimpleFunction<CsvRow, KV<String, KV<String, CsvRow>>> {

    abstract String primaryOrderKey();

    @Nullable
    abstract String secondaryOrderKey();

    @Override
    public KV<String, KV<String, CsvRow>> apply(CsvRow input) {
      var key1 = input.get(primaryOrderKey());
      var key2 =
          (secondaryOrderKey() != null) ? input.get(secondaryOrderKey()) : DEFAULT_SORT_KEY_VALUE;

      return KV.of(key1, KV.of(key2, input));
    }

    public static CsvRowToSortNameKV create(
        String primaryOrderKey, @Nullable String secondaryOrderKey) {
      return new AutoValue_SortCsvRow_CsvRowToSortNameKV(primaryOrderKey, secondaryOrderKey);
    }

    public static CsvRowToSortNameKV usingColumnNames(List<String> orderingColumns) {
      checkNotNull(orderingColumns);
      checkArgument(orderingColumns.size() > 0 && orderingColumns.size() <= 2);

      var primaryKey = orderingColumns.get(0);
      var secondaryKey = (orderingColumns.size() == 2) ? orderingColumns.get(1) : null;

      return create(primaryKey, secondaryKey);
    }
  }
}
