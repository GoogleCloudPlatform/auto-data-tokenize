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
import static org.apache.beam.sdk.io.FileIO.Write.defaultNaming;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.InspectionReport;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

@AutoValue
public abstract class InspectionReportFileWriter
    extends PTransform<PCollection<InspectionReport>, PDone> {

  abstract String reportLocation();

  public static InspectionReportFileWriter create(String reportLocation) {
    checkArgument(isNotBlank(reportLocation), "Report location is blank.");

    return new AutoValue_InspectionReportFileWriter(reportLocation);
  }

  @Override
  public PDone expand(PCollection<InspectionReport> inspectionReport) {
    PCollection<String> avroSchema =
        inspectionReport.apply("ExtractSchema", MapElements.via(new SchemaExtractor()));

    PCollection<ColumnInformation> columnInformation =
        inspectionReport
            .apply("ExtractColumnInformation", MapElements.via(new ColumnInformationExtractor()))
            .apply(Flatten.iterables());

    // Write the schema to a file.
    avroSchema.apply(
        "WriteSchema",
        FileIO.<String>write()
            .to(reportLocation())
            .via(TextIO.sink())
            .withNumShards(1)
            .withNaming((window, pane, numShards, shardIndex, compression) -> "schema.json"));

    // Write Column Information to GCS file.
    columnInformation.apply(
        "WriteColumnReport",
        FileIO.<String, ColumnInformation>writeDynamic()
            .via(
                Contextful.fn(JsonConvertor::asJsonString), Contextful.fn(colName -> TextIO.sink()))
            .by(ColumnInformation::getColumnName)
            .withDestinationCoder(StringUtf8Coder.of())
            .withNoSpilling()
            .withNaming(
                Contextful.fn(
                    colName ->
                        defaultNaming(
                            /* prefix= */ String.format(
                                    "col-%s", colName.replaceAll("[\\.\\$\\[\\]]+", "-"))
                                .replaceAll("[-]+", "-"),
                            /* suffix= */ ".json")))
            .to(reportLocation()));

    return PDone.in(inspectionReport.getPipeline());
  }

  private static final class SchemaExtractor extends SimpleFunction<InspectionReport, String> {

    @Override
    public String apply(InspectionReport input) {
      return input.getAvroSchema();
    }
  }

  private static final class ColumnInformationExtractor
      extends SimpleFunction<InspectionReport, Iterable<ColumnInformation>> {

    @Override
    public Iterable<ColumnInformation> apply(InspectionReport input) {

      if (input.getColumnReportCount() == 0) {
        return ImmutableList.of();
      }

      return input.getColumnReportList();
    }
  }
}
