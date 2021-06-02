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

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.InspectionReport;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** Converts the InspectionReport proto object to a {@link TableRow} JSON object. */
public final class InspectionReportToTableRow
    implements SerializableFunction<InspectionReport, TableRow> {

  public static InspectionReportToTableRow create() {
    return new InspectionReportToTableRow();
  }

  @Override
  public TableRow apply(InspectionReport report) {

    var tableRow =
        new TableRow()
            .set("timestamp", Timestamps.toString(report.getTimestamp()))
            .set("source_type", report.getSourceType().name())
            .set("input_pattern", report.getInputPattern())
            .set("avro_schema", report.getAvroSchema());

    if (report.hasJdbcConfiguration()) {
      tableRow.set(
          "jdbc_configuration",
          new TableRow()
              .set("connection_url", report.getJdbcConfiguration().getConnectionUrl())
              .set("driver_class_name", report.getJdbcConfiguration().getDriverClassName()));
    }

    if (report.getColumnReportCount() > 0) {
      tableRow.set(
          "column_report",
          report.getColumnReportList().stream()
              .map(
                  columnInformation ->
                      new TableRow()
                          .set("column_name", columnInformation.getColumnName())
                          .set(
                              "info_types",
                              columnInformation.getInfoTypesList().stream()
                                  .map(
                                      infoType ->
                                          new TableRow()
                                              .set("info_type", infoType.getInfoType())
                                              .set("count", infoType.getCount()))
                                  .collect(toImmutableList())))
              .collect(toImmutableList()));
    }

    return tableRow;
  }
}
