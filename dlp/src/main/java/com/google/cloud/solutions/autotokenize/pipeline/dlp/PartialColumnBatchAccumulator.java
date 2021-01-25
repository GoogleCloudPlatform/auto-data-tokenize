package com.google.cloud.solutions.autotokenize.pipeline.dlp;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.commons.lang3.StringUtils.isBlank;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.PartialColumnDlpTable;
import com.google.cloud.solutions.autotokenize.common.BatchAccumulator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public final class PartialColumnBatchAccumulator implements BatchAccumulator<FlatRecord, PartialColumnDlpTable> {

  public static final int MAX_DLP_PAYLOAD_SIZE_BYTES = 500000; //500kB
  public static final int MAX_DLP_PAYLOAD_CELLS = 50000; //50K

  private final int maxPayloadSize;
  private final int maxCellCount;
  private final DlpEncryptConfig encryptConfig;
  private final ImmutableSet<String> encryptColumns;
  private PartialColumnDlpTable accumulatedRecords;


  @VisibleForTesting
  PartialColumnBatchAccumulator(int maxPayloadSize, int maxCellCount, DlpEncryptConfig encryptConfig) {
    this.maxPayloadSize = maxPayloadSize;
    this.maxCellCount = maxCellCount;
    this.encryptConfig = encryptConfig;
    this.encryptColumns =
      encryptConfig
        .getTransformsList()
        .stream()
        .map(AutoTokenizeMessages.ColumnTransform::getColumnId)
        .collect(toImmutableSet());

    this.accumulatedRecords = PartialColumnDlpTable.getDefaultInstance();
  }

  public static PartialColumnBatchAccumulator withConfig(DlpEncryptConfig encryptConfig) {
    return new PartialColumnBatchAccumulator(MAX_DLP_PAYLOAD_SIZE_BYTES, MAX_DLP_PAYLOAD_CELLS, encryptConfig);
  }

  @Override
  public boolean addElement(FlatRecord element) {
    checkArgument(!isBlank(element.getRecordId()), "Provide FlatRecord with unique RecordId, found empty");

    Table updatedTable = new TableUpdater(element).updateTable();

    if (isTableWithinDlpLimits(updatedTable)) {
      accumulatedRecords =
        accumulatedRecords.toBuilder()
          .addRecords(element)
          .setTable(updatedTable)
          .build();

      return true;
    }

    return false;
  }

  @Override
  public BatchPartialColumnDlpTable makeBatch() {
    return BatchPartialColumnDlpTable.create(accumulatedRecords);
  }

  private class TableUpdater {

    private final FlatRecord element;

    public TableUpdater(FlatRecord element) {
      this.element = element;
    }

    private Table updateTable() {

      ImmutableList<String> existingHeaders = existingAccumulatedHeaders();

      Set<String> encryptColumnsInElement = extractEncryptColumns();

      Set<String> newHeaders =
        Sets.difference(encryptColumnsInElement, ImmutableSet.copyOf(existingHeaders)).immutableCopy();

      List<Table.Row> updatedRows = new RowsUpdater(existingHeaders, newHeaders).addRecord();
      List<FieldId> updatedHeaders =
        Stream.concat(existingHeaders.stream(), newHeaders.stream())
          .map(h -> FieldId.newBuilder().setName(h).build())
          .collect(toImmutableList());

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
        return Stream.concat(updatedRows(), Stream.of(convertToRow()))
          .collect(toImmutableList());
      }

      private Stream<Table.Row> updatedRows() {
        ImmutableList<Value> additionalCells = makeEmptyValues(newHeaders.size());

        return accumulatedRecords.getTable().getRowsList()
          .stream()
          .map(row -> row.toBuilder().addAllValues(additionalCells).build());
      }

      private ImmutableList<Value> makeEmptyValues(int count) {
        ImmutableList.Builder<Value> builder = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
          builder.add(Value.getDefaultInstance());
        }
        return builder.build();
      }


      public Table.Row convertToRow() {
        List<Value> rowValues =
          Stream.concat(existingHeaders.stream(), newHeaders.stream())
            .map(h -> element.getValuesMap().getOrDefault(h, Value.getDefaultInstance()))
            .collect(toImmutableList());

        return Table.Row.newBuilder().addAllValues(rowValues).build();
      }

    }


    private ImmutableSet<String> extractEncryptColumns() {
      return element.getFlatKeySchemaMap()
        .entrySet()
        .stream()
        .filter(entry -> encryptColumns.contains(entry.getValue()))
        .map(Map.Entry::getKey)
        .collect(toImmutableSet());
    }


    private ImmutableList<String> existingAccumulatedHeaders() {
      return accumulatedRecords.getTable().getHeadersList().stream().map(FieldId::getName).collect(toImmutableList());
    }
  }

  private boolean isTableWithinDlpLimits(Table table) {
    int cells = table.getHeadersCount() * (table.getRowsCount() + 1);
    return table.getSerializedSize() <=  maxPayloadSize && cells <= maxCellCount;
  }

  @AutoValue
  static abstract class BatchPartialColumnDlpTable implements Batch<PartialColumnDlpTable> {

    public static BatchPartialColumnDlpTable create(PartialColumnDlpTable partialColumnDlpTable) {

      int rows = partialColumnDlpTable.getRecordsCount();
      int columns = partialColumnDlpTable.getTable().getHeadersCount();
      int serializedSize = partialColumnDlpTable.getTable().getSerializedSize();

      String report = MoreObjects.toStringHelper(BatchPartialColumnDlpTable.class)
        .add("rows", rows)
        .add("columns", columns)
        .add("serializedSize", serializedSize)
        .toString();

      return new AutoValue_PartialColumnBatchAccumulator_BatchPartialColumnDlpTable(
        partialColumnDlpTable, rows, serializedSize, report);
    }
  }
}
