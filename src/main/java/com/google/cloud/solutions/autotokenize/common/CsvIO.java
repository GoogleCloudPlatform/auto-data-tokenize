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
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

import com.google.auto.value.AutoValue;
import com.google.common.base.Objects;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVFormat.Predefined;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Reads a CSV file and outputs a {@code PCollection} of {@link CsvRow}. You can optionally provide
 * your own headers.
 *
 * <pre>{@code
 * PCollection<FileIO.ReadableFile> readableFiles = ...;
 *
 * PCollection<CsvRow> csvRows = readableFiles.apply("ReadCsvRecords", CsvIO.readFiles());
 * }</pre>
 *
 * You can provide customizations like, providing the column delimiter character using {@link
 * CsvFilesRead#withDelimiter(Character)}
 */
public class CsvIO {

  public static CsvFilesRead readFiles() {
    return new AutoValue_CsvIO_CsvFilesRead.Builder()
        .csvFormatType(Predefined.Default.name())
        .fileCharset(StandardCharsets.UTF_8.name())
        .useFirstRowHeader(false)
        .delimiter(',')
        .build();
  }

  public static CsvRead read(String filePattern) {
    return new AutoValue_CsvIO_CsvRead.Builder()
        .filePattern(filePattern)
        .csvFormatType(Predefined.Default.name())
        .fileCharset(StandardCharsets.UTF_8.name())
        .delimiter(',')
        .useFirstRowHeader(false)
        .build();
  }

  public static <T> CsvParse<T> parse(
      String inputPattern, SerializableFunction<CsvRow, T> parseFn) {
    return new AutoValue_CsvIO_CsvParse.Builder<T>()
        .inputPattern(inputPattern)
        .parseFn(parseFn)
        .csvFormatType(Predefined.Default.name())
        .fileCharset(StandardCharsets.UTF_8.name())
        .useFirstRowHeader(false)
        .delimiter(',')
        .build();
  }

  public static <T> CsvParseFile<T> parseCsvFile(SerializableFunction<CsvRow, T> parseFn) {
    return new AutoValue_CsvIO_CsvParseFile.Builder<T>()
        .parseFn(parseFn)
        .csvFormatType(Predefined.Default.name())
        .fileCharset(StandardCharsets.UTF_8.name())
        .useFirstRowHeader(false)
        .delimiter(',')
        .build();
  }

  public static <T> CsvWrite<T> write(SerializableFunction<T, Iterable<CsvRow>> transformFn) {
    return CsvWrite.<T>builder().transformFunction(transformFn).build();
  }

  @AutoValue
  public abstract static class CsvRead extends PTransform<PBegin, PCollection<CsvRow>> {

    abstract String filePattern();

    abstract String fileCharset();

    abstract String csvFormatType();

    abstract Character delimiter();

    abstract @Nullable List<String> headers();

    abstract boolean useFirstRowHeader();

    @AutoValue.Builder
    abstract static class Builder {

      public abstract Builder filePattern(String filePattern);

      public abstract Builder fileCharset(String fileCharset);

      public abstract Builder csvFormatType(String csvFormatType);

      public abstract Builder delimiter(Character delimiter);

      public abstract Builder headers(@Nullable List<String> headers);

      public abstract Builder useFirstRowHeader(boolean useFirstRowHeader);

      abstract CsvRead autoBuild();

      public CsvRead build() {
        var auto = autoBuild();

        // Validate headers
        if (auto.headers() != null) {
          checkArgument(
              !auto.headers().isEmpty(), "User defined headers should be absent or non-empty");

          var emptyHeaders = auto.headers().stream().filter(StringUtils::isBlank).count();

          checkArgument(emptyHeaders == 0, "Please provide non-blank headers");
        }

        return auto;
      }
    }

    abstract Builder toBuilder();

    public CsvRead withFileCharSet(String charSet) {
      return toBuilder().fileCharset(charSet).build();
    }

    public CsvRead withCsvFormatType(String csvFormatType) {
      return toBuilder().csvFormatType(csvFormatType).build();
    }

    public CsvRead withDelimiter(Character delimiter) {
      return toBuilder().delimiter(delimiter).build();
    }

    public CsvRead withHeaders(List<String> headers) {
      return toBuilder().headers(headers).build();
    }

    public CsvRead withUseFirstRowHeader() {
      return withUseFirstRowHeader(true);
    }

    public CsvRead withUseFirstRowHeader(boolean useFirstRowHeader) {
      return toBuilder().useFirstRowHeader(useFirstRowHeader).build();
    }

    @Override
    public PCollection<CsvRow> expand(PBegin input) {

      return input
          .apply(FileIO.match().filepattern(filePattern()))
          .apply(FileIO.readMatches())
          .apply(
              readFiles()
                  .withFileCharSet(fileCharset())
                  .withCsvFormatType(csvFormatType())
                  .withDelimiter(delimiter())
                  .withHeaders(headers())
                  .withUseFirstRowHeader(useFirstRowHeader()))
          .setCoder(CsvRow.coder());
    }
  }

  @AutoValue
  public abstract static class CsvFilesRead
      extends PTransform<PCollection<ReadableFile>, PCollection<CsvRow>> {

    abstract String fileCharset();

    abstract String csvFormatType();

    abstract Character delimiter();

    abstract @Nullable List<String> headers();

    abstract boolean useFirstRowHeader();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder fileCharset(String fileCharset);

      public abstract Builder csvFormatType(String csvFormatType);

      public abstract Builder delimiter(Character delimiter);

      public abstract Builder headers(@Nullable List<String> headers);

      public abstract Builder useFirstRowHeader(boolean useFirstRowHeader);

      public abstract CsvFilesRead build();
    }

    abstract Builder toBuilder();

    public CsvFilesRead withFileCharSet(String charSet) {
      return toBuilder().fileCharset(charSet).build();
    }

    public CsvFilesRead withCsvFormatType(String csvFormatType) {
      return toBuilder().csvFormatType(csvFormatType).build();
    }

    public CsvFilesRead withDelimiter(Character delimiter) {
      return toBuilder().delimiter(delimiter).build();
    }

    public CsvFilesRead withHeaders(List<String> headers) {
      return toBuilder().headers(headers).build();
    }

    public CsvFilesRead withUseFirstRowHeader() {
      return withUseFirstRowHeader(true);
    }

    private CsvFilesRead withUseFirstRowHeader(boolean useFirstRowHeader) {
      return toBuilder().useFirstRowHeader(useFirstRowHeader).build();
    }

    @Override
    public PCollection<CsvRow> expand(PCollection<ReadableFile> input) {
      return input
          .apply(
              "ReadAndParseCsvFile",
              parseCsvFile(IdentityFn.of())
                  .withCsvFormatType(csvFormatType())
                  .withDelimiter(delimiter())
                  .withFileCharSet(fileCharset())
                  .withHeaders(headers())
                  .withUseFirstRowHeader(useFirstRowHeader()))
          .setCoder(CsvRow.coder());
    }
  }

  @AutoValue
  public abstract static class CsvParse<T> extends PTransform<PBegin, PCollection<T>> {

    abstract String inputPattern();

    abstract SerializableFunction<CsvRow, T> parseFn();

    abstract String fileCharset();

    abstract String csvFormatType();

    abstract Character delimiter();

    abstract @Nullable List<String> headers();

    abstract boolean useFirstRowHeader();

    @AutoValue.Builder
    public abstract static class Builder<T> {

      public abstract Builder<T> inputPattern(String inputPattern);

      public abstract Builder<T> parseFn(SerializableFunction<CsvRow, T> parseFn);

      public abstract Builder<T> fileCharset(String fileCharset);

      public abstract Builder<T> csvFormatType(String csvFormatType);

      public abstract Builder<T> delimiter(Character delimiter);

      public abstract Builder<T> headers(@Nullable List<String> headers);

      public abstract Builder<T> useFirstRowHeader(boolean useFirstRowHeader);

      public abstract CsvParse<T> build();
    }

    abstract Builder<T> toBuilder();

    public CsvParse<T> withFileCharSet(String charSet) {
      return toBuilder().fileCharset(charSet).build();
    }

    public CsvParse<T> withCsvFormatType(String csvFormatType) {
      return toBuilder().csvFormatType(csvFormatType).build();
    }

    public CsvParse<T> withDelimiter(Character delimiter) {
      return toBuilder().delimiter(delimiter).build();
    }

    public CsvParse<T> withHeaders(List<String> headers) {
      return toBuilder().headers(headers).build();
    }

    public CsvParse<T> withUseFirstRowHeader() {
      return withUseFirstRowHeader(true);
    }

    private CsvParse<T> withUseFirstRowHeader(boolean useFirstRowHeader) {
      return toBuilder().useFirstRowHeader(useFirstRowHeader).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      return input
          .apply(
              read(inputPattern())
                  .withFileCharSet(fileCharset())
                  .withCsvFormatType(csvFormatType())
                  .withDelimiter(delimiter())
                  .withHeaders(headers())
                  .withUseFirstRowHeader(useFirstRowHeader()))
          .apply("Parse", MapElements.into(TypeDescriptors.outputOf(parseFn())).via(parseFn()));
    }
  }

  @AutoValue
  public abstract static class CsvParseFile<T>
      extends PTransform<PCollection<ReadableFile>, PCollection<T>> {

    abstract SerializableFunction<CsvRow, T> parseFn();

    abstract String fileCharset();

    abstract String csvFormatType();

    abstract Character delimiter();

    abstract @Nullable List<String> headers();

    abstract boolean useFirstRowHeader();

    @AutoValue.Builder
    public abstract static class Builder<T> {

      public abstract Builder<T> parseFn(SerializableFunction<CsvRow, T> parseFn);

      public abstract Builder<T> fileCharset(String fileCharset);

      public abstract Builder<T> csvFormatType(String csvFormatType);

      public abstract Builder<T> delimiter(Character delimiter);

      public abstract Builder<T> headers(@Nullable List<String> headers);

      public abstract Builder<T> useFirstRowHeader(boolean useFirstRowHeader);

      public abstract CsvParseFile<T> build();
    }

    abstract Builder<T> toBuilder();

    public CsvParseFile<T> withFileCharSet(String charSet) {
      return toBuilder().fileCharset(charSet).build();
    }

    public CsvParseFile<T> withCsvFormatType(String csvFormatType) {
      return toBuilder().csvFormatType(csvFormatType).build();
    }

    public CsvParseFile<T> withDelimiter(Character delimiter) {
      return toBuilder().delimiter(delimiter).build();
    }

    public CsvParseFile<T> withHeaders(List<String> headers) {
      return toBuilder().headers(headers).build();
    }

    public CsvParseFile<T> withUseFirstRowHeader() {
      return withUseFirstRowHeader(true);
    }

    private CsvParseFile<T> withUseFirstRowHeader(boolean useFirstRowHeader) {
      return toBuilder().useFirstRowHeader(useFirstRowHeader).build();
    }

    @Override
    public PCollection<T> expand(PCollection<ReadableFile> input) {
      var csvFormat = CSVFormat.valueOf(csvFormatType());

      if (delimiter() != null) {
        csvFormat = csvFormat.withDelimiter(delimiter());
      }

      if (headers() != null) {
        csvFormat = csvFormat.withHeader(headers().toArray(new String[0]));
      }

      if (useFirstRowHeader()) {
        csvFormat = csvFormat.withFirstRecordAsHeader();
      }

      return input
          .apply("ReadCsvFile", ParDo.of(CsvFileReaderFn.create(fileCharset(), csvFormat)))
          .apply("Parse", MapElements.into(TypeDescriptors.outputOf(parseFn())).via(parseFn()));
    }
  }

  @AutoValue
  abstract static class CsvFileReaderFn extends DoFn<ReadableFile, CsvRow> {
    abstract String fileCharset();

    abstract CSVFormat csvFormat();

    public static CsvFileReaderFn create(String fileCharset, CSVFormat csvFormat) {
      return new AutoValue_CsvIO_CsvFileReaderFn(fileCharset, csvFormat);
    }

    @ProcessElement
    public void readCsvFile(@Element ReadableFile file, OutputReceiver<CsvRow> outputReceiver)
        throws IOException {
      var csvParser = buildCsvParser(file, fileCharset(), csvFormat());

      var headerMap = HashBiMap.create(firstNonNull(csvParser.getHeaderMap(), Map.of())).inverse();

      var csvParserHeadersPresent = (headerMap.size() > 0);

      for (var record : csvParser) {
        var row =
            (csvParserHeadersPresent)
                ? new CsvRow(record.toMap(), headerMap)
                : buildIndexNumberHeaderRow(record);

        outputReceiver.output(row);
      }
    }

    private CsvRow buildIndexNumberHeaderRow(CSVRecord csvRecord) {
      HashMap<String, String> valueMap = Maps.newHashMapWithExpectedSize(csvRecord.size());
      HashMap<Integer, String> indexMap = Maps.newHashMapWithExpectedSize(csvRecord.size());

      for (int index = 0; index < csvRecord.size(); index++) {
        var header = String.format("col_%s", index);
        valueMap.put(header, csvRecord.get(index));
        indexMap.put(index, header);
      }

      return new CsvRow(valueMap, indexMap);
    }

    private static CSVParser buildCsvParser(
        ReadableFile readableFile, String charset, CSVFormat csvFormat) throws IOException {
      return CSVParser.parse(
          Channels.newInputStream(readableFile.open()), Charset.forName(charset), csvFormat);
    }
  }

  @AutoValue
  public abstract static class CsvWrite<T>
      extends PTransform<PCollection<T>, WriteFilesResult<Void>> {

    @Nullable
    abstract String outputFilePrefix();

    abstract boolean writeHeaderRow();

    abstract CSVFormat csvFormat();

    abstract SerializableFunction<T, Iterable<CsvRow>> transformFunction();

    @Nullable
    abstract Integer fileShards();

    public CsvWrite<T> withFileShards(Integer fileShards) {
      if (fileShards == null) {
        return this;
      }

      checkArgument(fileShards > 0, "provide a positive fileShards count");
      return toBuilder().fileShards(fileShards).build();
    }

    public CsvWrite<T> withOutputFilePrefix(String outputFilePrefix) {
      return toBuilder().outputFilePrefix(outputFilePrefix).build();
    }

    public CsvWrite<T> withCsvFormat(CSVFormat csvFormat) {
      return toBuilder().csvFormat(csvFormat).build();
    }

    public CsvWrite<T> withCsvFormat(String csvFormat) {
      return toBuilder().csvFormat(CSVFormat.Predefined.valueOf(csvFormat).getFormat()).build();
    }

    public CsvWrite<T> withHeaderRow() {
      return toBuilder().writeHeaderRow(true).build();
    }

    @Override
    public WriteFilesResult<Void> expand(PCollection<T> input) {

      checkNotNull(outputFilePrefix(), "Provide outputFilePrefix for destination");

      var fileIO =
          FileIO.<T>write()
              .via(
                  CsvSink.<T>builder()
                      .csvFormat(csvFormat())
                      .transformFunction(transformFunction())
                      .writeHeaderRow(writeHeaderRow())
                      .build())
              .to(outputFilePrefix())
              .withSuffix(".csv");

      if (fileShards() != null) {
        fileIO = fileIO.withNumShards(fileShards());
      }

      return input.apply(fileIO);
    }

    public static <T> Builder<T> builder() {
      return new AutoValue_CsvIO_CsvWrite.Builder<T>()
          .writeHeaderRow(false)
          .csvFormat(CSVFormat.DEFAULT);
    }

    @AutoValue.Builder
    public abstract static class Builder<T> {

      public abstract Builder<T> outputFilePrefix(String outputFilePrefix);

      public abstract Builder<T> writeHeaderRow(boolean writeHeaderRow);

      public abstract Builder<T> csvFormat(CSVFormat csvFormat);

      public abstract Builder<T> transformFunction(
          SerializableFunction<T, Iterable<CsvRow>> transformFn);

      public abstract Builder<T> fileShards(Integer singleShard);

      public abstract CsvWrite<T> build();
    }

    abstract Builder<T> toBuilder();
  }

  @AutoValue
  public abstract static class CsvSink<T> implements FileIO.Sink<T> {

    abstract boolean writeHeaderRow();

    abstract CSVFormat csvFormat();

    abstract SerializableFunction<T, Iterable<CsvRow>> transformFunction();

    private CSVPrinter printer;
    private boolean isHeaderWritten = false;

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      printer = new CSVPrinter(new PrintWriter(Channels.newOutputStream(channel)), csvFormat());
    }

    @Override
    public void write(T element) throws IOException {

      var csvRows = ImmutableList.copyOf(transformFunction().apply(element));

      if (writeHeaderRow() && !isHeaderWritten) {
        printer.printRecord(csvRows.get(0).headersIterable());
        isHeaderWritten = true;
      }

      for (var row : csvRows) {
        printer.printRecord(row);
      }
    }

    @Override
    public void flush() throws IOException {
      printer.flush();
    }

    public static <T> Builder<T> builder() {
      return new AutoValue_CsvIO_CsvSink.Builder<>();
    }

    @AutoValue.Builder
    public abstract static class Builder<T> {

      public abstract Builder<T> writeHeaderRow(boolean writeHeaderRow);

      public abstract Builder<T> csvFormat(CSVFormat csvFormat);

      public abstract Builder<T> transformFunction(
          SerializableFunction<T, Iterable<CsvRow>> transformFn);

      public abstract CsvSink<T> build();
    }
  }

  /**
   * Immutable Map representing a Row in CSV file, the Key is either the column name or 0-based
   * index.
   */
  public static class CsvRow implements Serializable, Iterable<String> {

    private final LinkedHashMap<String, String> valueMap;

    private final TreeMap<Integer, String> indexNameMap;

    public CsvRow(Map<String, String> valueMap, Map<Integer, String> indexNameMap) {
      this.valueMap = new LinkedHashMap<>();
      this.indexNameMap = new TreeMap<>(indexNameMap);

      for (var indexEntry : indexNameMap.entrySet()) {
        this.valueMap.put(indexEntry.getValue(), valueMap.get(indexEntry.getValue()));
      }
    }

    public static Coder<CsvRow> coder() {
      return CsvRowCoder.of();
    }

    /** Returns the value for a the given columnName. {@code null} if not found. */
    public String get(String columnName) {
      return valueMap.get(columnName);
    }

    /** Returns the value for a the given 0-index columnNumber. {@code null} if not found. */
    public String get(Integer columnNumber) {
      var columnName = indexNameMap.get(columnNumber);
      return (columnName == null) ? null : get(columnName);
    }

    public SortedMap<Integer, String> headers() {
      return Collections.unmodifiableSortedMap(indexNameMap);
    }

    public int size() {
      return indexNameMap.size();
    }

    public Map<String, String> valuesMap() {
      return Collections.unmodifiableMap(valueMap);
    }

    @Override
    public Iterator<String> iterator() {
      return Iterators.unmodifiableIterator(valueMap.values().iterator());
    }

    public ImmutableList<String> headersIterable() {
      return indexNameMap.values().stream().collect(toImmutableList());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CsvRow csvRow = (CsvRow) o;
      return Objects.equal(valueMap, csvRow.valueMap)
          && Objects.equal(indexNameMap, csvRow.indexNameMap);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(valueMap, indexNameMap);
    }

    public static final class CsvRowCoder extends CustomCoder<CsvRow> {
      private static final Coder<Integer> INT_CODER = BigEndianIntegerCoder.of();
      private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

      public static CsvRowCoder of() {
        return new CsvRowCoder();
      }

      @Override
      public void encode(CsvRow csvRow, OutputStream outStream) throws IOException {
        var size = csvRow.indexNameMap.size();

        // Write Size;
        INT_CODER.encode(size, outStream);

        if (size == 0) {
          return;
        }

        var sortedIndexMap = new TreeMap<>(csvRow.indexNameMap);

        for (var indexEntry : sortedIndexMap.entrySet()) {
          var index = indexEntry.getKey();
          var columnName = indexEntry.getValue();
          var columnValue = csvRow.valueMap.get(columnName);

          INT_CODER.encode(index, outStream);
          STRING_CODER.encode(columnName, outStream);
          STRING_CODER.encode(columnValue, outStream);
        }
      }

      @Override
      public CsvRow decode(InputStream inStream) throws IOException {

        var size = INT_CODER.decode(inStream);

        HashMap<String, String> valueMap = Maps.newHashMapWithExpectedSize(size);
        HashMap<Integer, String> indexNameMap = Maps.newHashMapWithExpectedSize(size);

        for (int element = 0; element < size; element++) {

          var index = INT_CODER.decode(inStream);
          var columnName = STRING_CODER.decode(inStream);
          var columnValue = STRING_CODER.decode(inStream);

          indexNameMap.put(index, columnName);
          valueMap.put(columnName, columnValue);
        }

        return new CsvRow(valueMap, indexNameMap);
      }

      /**
       * It is a deterministic coder, by ensuring natural ordering of keys based on column Index.
       */
      @Override
      public void verifyDeterministic() {}
    }
  }
}
