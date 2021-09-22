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

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.solutions.autotokenize.common.CsvIO.CsvRow;
import com.google.cloud.solutions.autotokenize.testing.RecordsCountMatcher;
import com.google.cloud.solutions.autotokenize.testing.TestCsvFileGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class CsvIOTest {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void read_fileWithHeader_valid() throws IOException {

    var testCsvFile = temporaryFolder.newFile().getAbsolutePath();
    TestCsvFileGenerator.create().addHeaderRow().writeRandomRecordsToFile(testCsvFile);

    var csvRows = readPipeline.apply(CsvIO.read(testCsvFile).withUseFirstRowHeader());

    PAssert.that(csvRows)
        .satisfies(RecordsCountMatcher.hasRecordCount(TestCsvFileGenerator.DEFAULT_ROWS_COUNT));
    PAssert.that(csvRows)
        .satisfies(
            ValidateHeaders.forHeaders(
                ImmutableList.of("header-0", "header-1", "header-2", "header-3", "header-4")));

    readPipeline.run();
  }

  @Test
  public void read_fileWithoutHeader_headersNumersPrefixWithCol() throws IOException {

    var testCsvFile = temporaryFolder.newFile().getAbsolutePath();
    TestCsvFileGenerator.create().writeRandomRecordsToFile(testCsvFile);

    var csvRows = readPipeline.apply(CsvIO.read(testCsvFile));

    PAssert.that(csvRows)
        .satisfies(RecordsCountMatcher.hasRecordCount(TestCsvFileGenerator.DEFAULT_ROWS_COUNT));

    PAssert.that(csvRows)
        .satisfies(
            ValidateHeaders.forHeaders(
                ImmutableList.of("col_0", "col_1", "col_2", "col_3", "col_4")));

    readPipeline.run();
  }

  @Test
  public void read_fileWithoutHeader_containsAllValidRows() throws IOException {

    var testCsvFile = temporaryFolder.newFile().getAbsolutePath();
    var testCsvRecords = TestCsvFileGenerator.create().writeRandomRecordsToFile(testCsvFile);

    var csvRows =
        readPipeline
            .apply(CsvIO.read(testCsvFile))
            .apply("Convert To List Of Strings", MapElements.via(new CsvRowToListStringFn()));

    PAssert.that(csvRows).containsInAnyOrder(testCsvRecords);

    readPipeline.run();
  }

  @Test
  public void read_fileWithoutHeaders_validCsvRow() throws IOException {
    var testCsvRecords = List.of(List.of("col1", "col2", "col3", "col4"));
    var testCsvFile = temporaryFolder.newFile().getAbsolutePath();
    TestCsvFileGenerator.create().writeRecordsToFile(testCsvRecords, testCsvFile);

    var csvRows = readPipeline.apply(CsvIO.read(testCsvFile));

    PAssert.that(csvRows).satisfies(new ValidateCsvRowsAsExpected(testCsvRecords));
    PAssert.that(csvRows).satisfies(new ValidateHeadersContainsColPrefix());

    readPipeline.run();
  }

  @Test
  public void read_fileWithoutHeadersColonSeparator_validCsvRow() throws IOException {
    var testCsvRecords = List.of(List.of("col1", "col2", "col3", "col4"));
    var testCsvFile = temporaryFolder.newFile().getAbsolutePath();
    TestCsvFileGenerator.create()
        .withDelimiter(':')
        .writeRecordsToFile(testCsvRecords, testCsvFile);

    var csvRows = readPipeline.apply(CsvIO.read(testCsvFile).withDelimiter(':'));

    PAssert.that(csvRows).satisfies(new ValidateCsvRowsAsExpected(testCsvRecords));
    PAssert.that(csvRows).satisfies(new ValidateHeadersContainsColPrefix());

    readPipeline.run();
  }

  @Test
  public void read_fileNoHeadersManualProvidedHeadersLessThanActualColumn_valid()
      throws IOException {
    var testCsvFile = temporaryFolder.newFile().getAbsolutePath();
    TestCsvFileGenerator.create().writeRandomRecordsToFile(testCsvFile);
    var providedHeaders = List.of("myHeader1", "myHeader2", "myHeader3", "myHeader4", "myHeader5");

    var csvRows = readPipeline.apply(CsvIO.read(testCsvFile).withHeaders(providedHeaders));

    PAssert.that(csvRows).satisfies(ValidateHeaders.forHeaders(providedHeaders));
    readPipeline.run();
  }

  @Test
  public void read_manualProvidedHeadersMoreThanActualColumns_throwsNullPointerException()
      throws IOException {
    var testCsvFile = temporaryFolder.newFile().getAbsolutePath();
    TestCsvFileGenerator.create().withColumnCount(2).writeRandomRecordsToFile(testCsvFile);
    var providedHeaders = List.of("myHeader1", "myHeader2", "myHeader3");

    readPipeline.apply(CsvIO.read(testCsvFile).withHeaders(providedHeaders));

    var pipelineException =
        assertThrows(PipelineExecutionException.class, () -> readPipeline.run());

    assertThat(pipelineException).hasMessageThat().endsWith("cannot encode a null String");
  }

  @Test
  public void read_fileHeadersAndManualProvidedHeader_headersContainManualProvided()
      throws IOException {
    var testCsvFile = temporaryFolder.newFile().getAbsolutePath();
    TestCsvFileGenerator.create()
        .withColumnCount(2)
        .addHeaderRow()
        .writeRandomRecordsToFile(testCsvFile);
    var providedHeaders = List.of("myHeader1", "myHeader2");

    var csvRows = readPipeline.apply(CsvIO.read(testCsvFile).withHeaders(providedHeaders));

    PAssert.that(csvRows).satisfies(ValidateHeaders.forHeaders(providedHeaders));
    readPipeline.run();
  }

  @Test
  public void read_patternMatchesMultipleFiles_readsAllRows() throws IOException {
    var csvGenerator =
        TestCsvFileGenerator.create().withColumnCount(2).withRowCount(100).addHeaderRow();
    var folder = temporaryFolder.newFolder().getAbsolutePath();

    ArrayList<List<String>> testRows = new ArrayList<>();

    var rowsFile1 = csvGenerator.writeRandomRecordsToFile(folder + "/file1");
    var rowsFile2 = csvGenerator.writeRandomRecordsToFile(folder + "/file2");
    rowsFile1.remove(0); // Remove header
    rowsFile2.remove(0); // Remove header
    testRows.addAll(rowsFile1);
    testRows.addAll(rowsFile2);

    var csvRows = readPipeline.apply(CsvIO.read(folder + "/*").withUseFirstRowHeader());
    var csvColumnList = csvRows.apply(MapElements.via(new CsvRowToListStringFn()));

    PAssert.that(csvRows).satisfies(RecordsCountMatcher.hasRecordCount(200));
    PAssert.that(csvRows).satisfies(ValidateHeaders.forHeaders(List.of("header-0", "header-1")));
    PAssert.that(csvColumnList).containsInAnyOrder(testRows);

    readPipeline.run();
  }

  @Test
  public void parse_patternMatchesMultipleFiles_transformsAllRows() throws IOException {
    var csvGenerator =
        TestCsvFileGenerator.create().withColumnCount(2).withRowCount(100).addHeaderRow();
    var folder = temporaryFolder.newFolder().getAbsolutePath();

    ArrayList<List<String>> testRows = new ArrayList<>();

    var rowsFile1 = csvGenerator.writeRandomRecordsToFile(folder + "/file1");
    var rowsFile2 = csvGenerator.writeRandomRecordsToFile(folder + "/file2");
    rowsFile1.remove(0); // Remove header
    rowsFile2.remove(0); // Remove header
    testRows.addAll(rowsFile1);
    testRows.addAll(rowsFile2);

    var csvColumnList =
        readPipeline
            .apply(CsvIO.parse(folder + "/*", new CsvRowToListStringFn()).withUseFirstRowHeader())
            .setCoder(ListCoder.of(StringUtf8Coder.of()));

    PAssert.that(csvColumnList).satisfies(RecordsCountMatcher.hasRecordCount(200));
    PAssert.that(csvColumnList).containsInAnyOrder(testRows);

    readPipeline.run();
  }

  private static class CsvRowToListStringFn extends SimpleFunction<CsvRow, List<String>> {
    @Override
    public List<String> apply(CsvRow input) {
      return ImmutableList.copyOf(input);
    }
  }

  private static class ValidateCsvRowsAsExpected
      implements SerializableFunction<Iterable<CsvRow>, Void> {

    private final List<List<String>> expectedCsv;

    public ValidateCsvRowsAsExpected(List<List<String>> expectedCsv) {
      this.expectedCsv = expectedCsv;
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public Void apply(Iterable<CsvRow> input) {

      var inputRows = ImmutableList.copyOf(input);

      assertThat(inputRows).hasSize(expectedCsv.size());

      Streams.zip(
          inputRows.stream(),
          expectedCsv.stream(),
          (actualRow, expectedRow) -> assertThat(actualRow).containsExactly(expectedRow));

      return null;
    }
  }

  private static class ValidateHeadersContainsColPrefix
      implements SerializableFunction<Iterable<CsvRow>, Void> {

    @Override
    public Void apply(Iterable<CsvRow> input) {
      StreamSupport.stream(input.spliterator(), false)
          .map(CsvRow::headers)
          .map(Map::values)
          .flatMap(Collection::stream)
          .forEach(header -> assertThat(header).matches("^col_\\d+$"));

      return null;
    }
  }

  private static class ValidateHeaders implements SerializableFunction<Iterable<CsvRow>, Void> {

    private final ImmutableMap<Integer, String> expectedHeaders;

    @SuppressWarnings("UnstableApiUsage")
    private ValidateHeaders(List<String> headers) {
      this.expectedHeaders =
          Streams.mapWithIndex(
                  headers.stream(),
                  (header, index) -> ImmutablePair.of(Long.valueOf(index).intValue(), header))
              .collect(toImmutableMap(ImmutablePair::getLeft, ImmutablePair::getRight));
    }

    public static ValidateHeaders forHeaders(List<String> headers) {
      return new ValidateHeaders(headers);
    }

    @Override
    public Void apply(Iterable<CsvRow> input) {

      for (var row : input) {
        var headers = row.headers();
        assertThat(headers).containsExactlyEntriesIn(expectedHeaders);
      }

      return null;
    }
  }
}
