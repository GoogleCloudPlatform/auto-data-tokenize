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

package com.google.cloud.solutions.autotokenize.pipeline;

import static com.google.cloud.solutions.autotokenize.common.JsonConvertor.asJsonString;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnTransform;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.common.SecretsClient;
import com.google.cloud.solutions.autotokenize.dlp.PartialBatchAccumulator;
import com.google.cloud.solutions.autotokenize.pipeline.CsvTokenizationAndOrderingPipeline.CsvTokenizationAndOrderingPipelineOptions;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.cloud.solutions.autotokenize.testing.stubs.dlp.Base64EncodingDlpStub;
import com.google.cloud.solutions.autotokenize.testing.stubs.dlp.StubbingDlpClientFactory;
import com.google.cloud.solutions.autotokenize.testing.stubs.kms.Base64DecodingKmsStub;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public final class CsvTokenizationAndOrderingPipelineTest {

  @RunWith(Parameterized.class)
  public static final class ValidTests {

    //    private final ObjectWriter jsonWriter = new ObjectMapper().writer();

    @Rule public transient TestPipeline testPipeline = TestPipeline.create();

    @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final String inputFilePath;

    private final String expectedFilePath;

    private final DlpEncryptConfig encryptConfig;

    private final ImmutableList<String> deidColumns;

    private final int expectedFileShards;

    private final int primaryOrderingColumn;

    private final String[] testArgs;

    private CsvTokenizationAndOrderingPipelineOptions testOptions;

    private String testInputFolder;
    private String testOutputFolder;
    private String testTempFolder;

    public ValidTests(
        String testCaseName,
        String inputFilePath,
        String expectedFilePath,
        DlpEncryptConfig encryptConfig,
        ImmutableList<String> deidColumns,
        int expectedFileShards,
        String[] testArgs) {
      this.inputFilePath = inputFilePath;
      this.expectedFilePath = expectedFilePath;
      this.encryptConfig = encryptConfig;
      this.deidColumns = deidColumns;
      this.expectedFileShards = expectedFileShards;
      this.primaryOrderingColumn = 0;
      this.testArgs = testArgs;
    }

    @Before
    public void createTestDirectory() throws IOException {
      testInputFolder = temporaryFolder.newFolder("input").getAbsolutePath();
      testOutputFolder = temporaryFolder.newFolder("output").getAbsolutePath();
      testTempFolder = temporaryFolder.newFolder("temp").getAbsolutePath();
    }

    @Before
    public void makeOptions() throws IOException {
      testOptions =
          PipelineOptionsFactory.fromArgs(makeArgs())
              .as(CsvTokenizationAndOrderingPipelineOptions.class);
    }

    @Test
    public void buildPipeline_valid() throws Exception {

      var dlpStub =
          new Base64EncodingDlpStub(
              PartialBatchAccumulator.RECORD_ID_COLUMN_NAME,
              deidColumns,
              testOptions.getProject(),
              testOptions.getDlpRegion());

      // Build Pipeline
      new CsvTokenizationAndOrderingPipeline(
              testOptions,
              testPipeline,
              new StubbingDlpClientFactory(dlpStub),
              SecretsClient.of(),
              KeyManagementServiceClient.create(
                  new Base64DecodingKmsStub(testOptions.getMainKmsKeyUri())))
          .run()
          .waitUntilFinish();

      // Read output and verify
      var outputRecords = readFilesAsGrouped(listOutputFiles(), primaryOrderingColumn);
      var expectedFile =
          TestResourceLoader.classPath()
              .copyTo(new File(testTempFolder))
              .createFileTestCopy(expectedFilePath)
              .getAbsolutePath();

      if (expectedFileShards != -1) {
        assertThat(listOutputFiles()).hasLength(expectedFileShards);
      }

      if (testOptions.getOrderingColumns() != null
          || testOptions.getOrderingColumnNames() != null) {
        var expectedRecords = readFilesAsGrouped(new String[] {expectedFile}, 0);
        assertThat(outputRecords).containsExactlyEntriesIn(expectedRecords);
      } else {
        var outputRows =
            outputRecords.values().stream().flatMap(List::stream).collect(Collectors.toList());
        assertThat(outputRows).containsExactlyElementsIn(readCsvFileAsRecords(expectedFile));
      }
    }

    @Parameters(name = "{0}")
    public static ImmutableList<Object[]> validParams() {
      return ImmutableList.of(
          new Object[] {
            "Basic Test, output is ordered",
            "csv/sample-data-chats.csv",
            "csv/redacted-sorted-sample-data-chats.csv",
            DlpEncryptConfig.newBuilder()
                .addAllTransforms(
                    ImmutableList.of(
                        ColumnTransform.newBuilder().setColumnId("$.CsvRecord.col_2").build()))
                .build(),
            /*deidColumns=*/ ImmutableList.of("$.col_2"),
            /*expectedFileShards=*/ -1,
            new String[] {"--orderingColumns=0", "--orderingColumns=3"}
          },
          new Object[] {
            "Single Output Shard",
            "csv/sample-data-chats.csv",
            "csv/redacted-timestamp-sorted-sample-data-chats.csv",
            DlpEncryptConfig.newBuilder()
                .addAllTransforms(
                    ImmutableList.of(
                        ColumnTransform.newBuilder().setColumnId("$.CsvRecord.col_2").build()))
                .build(),
            /*deidColumns=*/ ImmutableList.of("$.col_2"),
            /*expectedFileShards=*/ 1,
            new String[] {"--orderingColumns=0", "--orderingColumns=4", "--csvFileShardCount=1"}
          },
          new Object[] {
            "sorting based on predefined headers",
            "csv/sample-data-chats.csv",
            "csv/redacted-timestamp-sorted-sample-data-chats.csv",
            DlpEncryptConfig.newBuilder()
                .addAllTransforms(
                    ImmutableList.of(
                        ColumnTransform.newBuilder().setColumnId("$.CsvRecord.transcript").build()))
                .build(),
            /*deidColumns=*/ ImmutableList.of("$.transcript"),
            /*expectedFileShards=*/ -1,
            new String[] {
              "--csvHeaders=chatId,userType,transcript,segmentId,segmentTimestamp",
              "--orderingColumnNames=chatId",
              "--orderingColumnNames=segmentTimestamp"
            }
          },
          new Object[] {
            "predefined headers no sorting",
            "csv/sample-data-chats.csv",
            "csv/redacted-timestamp-sorted-sample-data-chats.csv",
            DlpEncryptConfig.newBuilder()
                .addAllTransforms(
                    ImmutableList.of(
                        ColumnTransform.newBuilder().setColumnId("$.CsvRecord.transcript").build()))
                .build(),
            /*deidColumns=*/ ImmutableList.of("$.transcript"),
            /*expectedFileShards=*/ -1,
            new String[] {"--csvHeaders=chatId,userType,transcript,segmentId,segmentTimestamp"}
          });
    }

    private String[] makeArgs() throws IOException {
      var argsBuilder = ImmutableList.<String>builder().add(testArgs);

      var inputFile =
          TestResourceLoader.classPath()
              .copyTo(new File(testInputFolder))
              .createFileTestCopy(inputFilePath);

      argsBuilder
          .add("--inputPattern=" + inputFile.getAbsolutePath())
          .add("--outputDirectory=" + testOutputFolder)
          .add("--dlpEncryptConfigJson=" + asJsonString(encryptConfig))
          .add("--tempLocation=" + testTempFolder)
          .add("--project=test-project");

      return argsBuilder.build().toArray(new String[0]);
    }

    private String[] listOutputFiles() {
      var files =
          new File(testOutputFolder)
              .list((File directory, String fileName) -> fileName.endsWith("csv"));
      return (files == null)
          ? new String[0]
          : Arrays.stream(files)
              .map(fileName -> testOutputFolder + "/" + fileName)
              .sorted()
              .toArray(String[]::new);
    }

    private Map<String, List<ImmutableList<String>>> readFilesAsGrouped(
        String[] filesArray, int groupingColumn) {
      // primaryOrderingColumn
      return Arrays.stream(filesArray)
          .flatMap(file -> readCsvFileAsRecords(file).stream())
          .collect(Collectors.groupingBy(row -> row.get(groupingColumn)));
    }

    private static ImmutableList<ImmutableList<String>> readCsvFileAsRecords(String file) {

      ImmutableList.Builder<ImmutableList<String>> rowsBuilder = ImmutableList.builder();

      try (CSVParser parser =
          CSVParser.parse(new FileReader(file, StandardCharsets.UTF_8), CSVFormat.DEFAULT)) {
        parser.forEach(row -> rowsBuilder.add(ImmutableList.copyOf(row)));
      } catch (IOException ioException) {
        System.out.println("Error processing file: " + file);
      }

      return rowsBuilder.build();
    }
  }
}
