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

package com.google.cloud.solutions.autotokenize.testing;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toList;

import com.google.common.flogger.GoogleLogger;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public final class TestCsvFileGenerator {

  public static final int DEFAULT_COLUMN_COUNT = 5;
  public static final int DEFAULT_ROWS_COUNT = 100;

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final int numberOfRows;
  private final int numColumns;
  private final boolean addHeader;
  private final char delimiter;

  private TestCsvFileGenerator(
      int numberOfRows, int numColumns, boolean addHeader, char delimiter) {
    this.numberOfRows = numberOfRows;
    this.numColumns = numColumns;
    this.addHeader = addHeader;
    this.delimiter = delimiter;
  }

  public static TestCsvFileGenerator create() {
    return new TestCsvFileGenerator(DEFAULT_ROWS_COUNT, DEFAULT_COLUMN_COUNT, false, ',');
  }

  public TestCsvFileGenerator addHeaderRow() {
    return new TestCsvFileGenerator(numberOfRows, numColumns, true, delimiter);
  }

  public TestCsvFileGenerator withRowCount(int numberOfRows) {
    return new TestCsvFileGenerator(numberOfRows, numColumns, addHeader, delimiter);
  }

  public TestCsvFileGenerator withColumnCount(int numColumns) {
    return new TestCsvFileGenerator(numberOfRows, numColumns, addHeader, delimiter);
  }

  public TestCsvFileGenerator withDelimiter(char delimiter) {
    return new TestCsvFileGenerator(numberOfRows, numColumns, addHeader, delimiter);
  }

  public List<List<String>> writeRandomRecordsToFile(String fileName) throws IOException {
    logger.atInfo().log(
        "Creating %s records (headers(%s): %s) in fileName %s",
        numberOfRows, addHeader, numColumns, fileName);

    var headers =
        IntStream.range(0, numColumns).mapToObj(x -> "header-" + x).collect(toImmutableList());

    var randomStringGenerator = new RandomStringGenerator();

    var csvData =
        Stream.concat(
                (addHeader ? Stream.of(headers) : Stream.of()),
                IntStream.range(0, numberOfRows)
                    .mapToObj(
                        x ->
                            IntStream.range(0, numColumns)
                                .boxed()
                                .map(randomStringGenerator::buildRandomString)
                                .collect(toList())))
            .collect(toList());

    writeRecordsToFile(csvData, fileName);
    return csvData;
  }

  public void writeRecordsToFile(List<List<String>> csvRecords, String fileName)
      throws IOException {

    try (var csvPrinter =
        new CSVPrinter(new FileWriter(fileName), CSVFormat.DEFAULT.withDelimiter(delimiter))) {
      for (var testRow : csvRecords) {
        csvPrinter.printRecord(testRow);
      }

      csvPrinter.close(true);
    }
  }

  static class RandomStringGenerator {
    private final Random random;

    RandomStringGenerator() {
      random = new Random();
    }

    String buildRandomString(int x) {
      return random
          .ints('a', 'z' + 1)
          .limit(random.nextInt(20))
          .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
          .toString();
    }

    String buildRandomString() {
      return buildRandomString(0);
    }
  }
}
