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
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.commons.lang3.StringUtils.isBlank;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.PartialColumnDlpTable;
import com.google.cloud.solutions.autotokenize.common.DeidentifyColumns;
import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.RecordTransformations;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@AutoValue
public abstract class PartialColumnBatchAccumulator
    implements BatchAccumulator<FlatRecord, PartialColumnDlpTable> {

  public static final String RECORD_ID_COLUMN_NAME = "__AUTOTOKENIZE__RECORD_ID__";
  public static final int MAX_DLP_PAYLOAD_SIZE_BYTES = 500000; // 500kB
  public static final int MAX_DLP_PAYLOAD_CELLS = 50000; // 50K

  private static final Pattern ARRAY_INDEX_PATTERN = Pattern.compile("\\[\\d+\\]");

  abstract int maxPayloadSize();

  abstract int maxCellCount();

  abstract String recordIdColumnName();

  abstract DlpEncryptConfig dlpEncryptConfig();

  abstract ImmutableSet<String> encryptColumns();

  private PartialColumnDlpTable accumulatedRecords = PartialColumnDlpTable.getDefaultInstance();

  private final Multimap<String, String> columnSchemaKeyMap = HashMultimap.create();

  public static final class PartialColumnBatchAccumulatorFactory
      implements BatchAccumulatorFactory<FlatRecord, PartialColumnDlpTable> {
    private final DlpEncryptConfig encryptConfig;

    private PartialColumnBatchAccumulatorFactory(DlpEncryptConfig encryptConfig) {
      this.encryptConfig = encryptConfig;
    }

    @Override
    public PartialColumnBatchAccumulator newAccumulator() {
      return withConfig(encryptConfig);
    }
  }

  public static PartialColumnBatchAccumulatorFactory factory(DlpEncryptConfig encryptConfig) {
    return new PartialColumnBatchAccumulatorFactory(encryptConfig);
  }

  public static PartialColumnBatchAccumulator withConfig(DlpEncryptConfig encryptConfig) {
    ImmutableSet<String> encryptColumnNames =
        encryptConfig.getTransformsList().stream()
            .map(AutoTokenizeMessages.ColumnTransform::getColumnId)
            .collect(toImmutableSet());

    return builder().dlpEncryptConfig(encryptConfig).encryptColumns(encryptColumnNames).build();
  }

  private static Builder builder() {
    return new AutoValue_PartialColumnBatchAccumulator.Builder()
        .maxCellCount(MAX_DLP_PAYLOAD_CELLS)
        .maxPayloadSize(MAX_DLP_PAYLOAD_SIZE_BYTES)
        .recordIdColumnName(RECORD_ID_COLUMN_NAME);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder maxPayloadSize(int maxPayloadSize);

    public abstract Builder maxCellCount(int maxCellCount);

    public abstract Builder recordIdColumnName(String recordIdColumnName);

    public abstract Builder dlpEncryptConfig(DlpEncryptConfig dlpEncryptConfig);

    public abstract Builder encryptColumns(ImmutableSet<String> encryptColumns);

    public abstract PartialColumnBatchAccumulator build();
  }

  abstract Builder toBuilder();

  public PartialColumnBatchAccumulator withMaxPayloadSize(int maxPayloadSize) {
    return toBuilder().maxPayloadSize(maxPayloadSize).build();
  }

  public PartialColumnBatchAccumulator withMaxCellCount(int maxCellCount) {
    return toBuilder().maxCellCount(maxCellCount).build();
  }

  public PartialColumnBatchAccumulator withRecordIdColumnName(String recordIdColumnName) {
    return toBuilder().recordIdColumnName(recordIdColumnName).build();
  }

  @Override
  public final boolean addElement(FlatRecord element) {
    checkNotNull(element, "Null element not allowed");
    checkArgument(
        !isBlank(element.getRecordId()), "Provide FlatRecord with unique RecordId, found empty");

    var updatedTable = new TableUpdater(element).updateTable();

    if (isTableWithinDlpLimits(updatedTable)) {
      accumulatedRecords =
          accumulatedRecords.toBuilder().addRecords(element).setTable(updatedTable).build();

      updateElementColumnKeys(element);
      return true;
    }

    return false;
  }

  private void updateElementColumnKeys(FlatRecord element) {
    element.getFlatKeySchemaMap().forEach((key, value) -> columnSchemaKeyMap.put(value, key));
  }

  @Override
  public final BatchPartialColumnDlpTable makeBatch() {
    PartialColumnDlpTable tableWithDeidentifyConfig =
        accumulatedRecords.toBuilder()
            .setRecordIdColumnName(recordIdColumnName())
            .setDeidentifyConfig(buildTableDeidentifyConfig())
            .build();

    return BatchPartialColumnDlpTable.create(tableWithDeidentifyConfig);
  }

  private DeidentifyConfig buildTableDeidentifyConfig() {

    ImmutableList<FieldTransformation> fieldTransformations =
        dlpEncryptConfig().getTransformsList().stream()
            .map(
                colEncryptConfig ->
                    FieldTransformation.newBuilder()
                        .addAllFields(
                            DeidentifyColumns.fieldIdsFor(
                                columnSchemaKeyMap.get(colEncryptConfig.getColumnId()).stream()
                                    .map(
                                        colName ->
                                            ARRAY_INDEX_PATTERN.matcher(colName).replaceAll(""))
                                    .distinct()
                                    .collect(toImmutableList())))
                        .setPrimitiveTransformation(colEncryptConfig.getTransform())
                        .build())
            .collect(toImmutableList());

    return DeidentifyConfig.newBuilder()
        .setRecordTransformations(
            RecordTransformations.newBuilder().addAllFieldTransformations(fieldTransformations))
        .build();
  }

  private class TableUpdater {

    private final FlatRecord element;

    public TableUpdater(FlatRecord element) {
      this.element = element;
    }

    private Table updateTable() {
      ImmutableList<String> existingHeaders = existingAccumulatedHeaders();

      Set<String> encryptColumnsInElement =
          Sets.union(ImmutableSet.of(recordIdColumnName()), extractEncryptColumns())
              .immutableCopy();

      Set<String> newHeaders =
          Sets.difference(encryptColumnsInElement, ImmutableSet.copyOf(existingHeaders))
              .immutableCopy();

      List<Table.Row> updatedRows = new RowsUpdater(existingHeaders, newHeaders).addRecord();
      List<FieldId> updatedHeaders =
          DeidentifyColumns.fieldIdsFrom(
              Stream.concat(existingHeaders.stream(), newHeaders.stream()));

      return Table.newBuilder().addAllHeaders(updatedHeaders).addAllRows(updatedRows).build();
    }

    private class RowsUpdater {

      private final ImmutableList<String> existingHeaders;
      private final ImmutableList<String> newHeaders;

      public RowsUpdater(ImmutableList<String> existingHeaders, Collection<String> newHeaders) {
        this.existingHeaders = existingHeaders;
        this.newHeaders = ImmutableList.copyOf(newHeaders);
      }

      public List<Table.Row> addRecord() {
        return Stream.concat(updatedRows(), Stream.of(convertElementToRow()))
            .collect(toImmutableList());
      }

      private Stream<Table.Row> updatedRows() {
        ImmutableList<Value> additionalCells = makeEmptyValues(newHeaders.size());

        return accumulatedRecords.getTable().getRowsList().stream()
            .map(row -> row.toBuilder().addAllValues(additionalCells).build());
      }

      private ImmutableList<Value> makeEmptyValues(int count) {
        ImmutableList.Builder<Value> builder = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
          builder.add(Value.getDefaultInstance());
        }
        return builder.build();
      }

      public Table.Row convertElementToRow() {
        List<Value> rowValues =
            Stream.concat(existingHeaders.stream(), newHeaders.stream())
                .map(
                    h -> {
                      if (h.equals(recordIdColumnName())) {
                        return Value.newBuilder().setStringValue(element.getRecordId()).build();
                      }
                      return element.getValuesMap().getOrDefault(h, Value.getDefaultInstance());
                    })
                .collect(toImmutableList());

        return Table.Row.newBuilder().addAllValues(rowValues).build();
      }
    }

    private ImmutableSet<String> extractEncryptColumns() {
      return element.getFlatKeySchemaMap().entrySet().stream()
          .filter(entry -> encryptColumns().contains(entry.getValue()))
          .map(Map.Entry::getKey)
          .collect(toImmutableSet());
    }

    private ImmutableList<String> existingAccumulatedHeaders() {
      return DeidentifyColumns.columnNamesIn(accumulatedRecords.getTable());
    }
  }

  private boolean isTableWithinDlpLimits(Table table) {
    int cells = table.getHeadersCount() * (table.getRowsCount() + 1);
    return table.getSerializedSize() <= maxPayloadSize() && cells <= maxCellCount();
  }

  @AutoValue
  abstract static class BatchPartialColumnDlpTable implements Batch<PartialColumnDlpTable> {

    public static BatchPartialColumnDlpTable create(PartialColumnDlpTable partialColumnDlpTable) {

      int rows = partialColumnDlpTable.getRecordsCount();
      int columns = partialColumnDlpTable.getTable().getHeadersCount();
      int serializedSize = partialColumnDlpTable.getTable().getSerializedSize();

      var report =
          MoreObjects.toStringHelper(BatchPartialColumnDlpTable.class)
              .add("rows", rows)
              .add("columns", columns)
              .add("serializedSize", serializedSize)
              .toString();

      return new AutoValue_PartialColumnBatchAccumulator_BatchPartialColumnDlpTable(
          partialColumnDlpTable, rows, serializedSize, report);
    }

    protected BatchPartialColumnDlpTable() {}
  }
}
