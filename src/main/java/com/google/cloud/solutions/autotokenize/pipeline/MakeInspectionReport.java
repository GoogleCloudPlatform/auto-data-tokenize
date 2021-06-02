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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.InspectionReport;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.JdbcConfiguration;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.protobuf.util.Timestamps;
import java.time.Clock;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Combines the DLP Identified sensitivity categorization and Schema into a report. */
@AutoValue
public abstract class MakeInspectionReport
    extends PTransform<PCollection<ColumnInformation>, PCollection<InspectionReport>> {

  private static final String SCHEMA_SIDE_INPUT_TAG = "schemaTag";

  abstract PCollectionView<String> avroSchema();

  abstract SourceType sourceType();

  abstract String inputPattern();

  abstract @Nullable JdbcConfiguration jdbcConfiguration();

  abstract Clock clock();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setAvroSchema(PCollectionView<String> value);

    public abstract Builder setSourceType(SourceType value);

    public abstract Builder setInputPattern(String value);

    public abstract Builder setJdbcConfiguration(JdbcConfiguration value);

    public abstract Builder setClock(Clock value);

    abstract MakeInspectionReport autoBuild();

    public MakeInspectionReport build() {
      var transform = autoBuild();
      checkArgument(
          !transform.sourceType().equals(SourceType.JDBC_TABLE)
              || transform.jdbcConfiguration() != null,
          "Provide JDBC Configuration");
      return transform;
    }
  }

  public static Builder builder() {
    return new AutoValue_MakeInspectionReport.Builder().setClock(Clock.systemUTC());
  }

  @Override
  public PCollection<InspectionReport> expand(PCollection<ColumnInformation> sensitiveColumns) {
    return sensitiveColumns
        .apply(WithKeys.of("const_key"))
        .apply(GroupByKey.create())
        .apply(Values.create())
        .apply(
            "BuildInspectionReport",
            ParDo.of(
                    ReportUnificationFn.create(
                        sourceType(), inputPattern(), clock(), jdbcConfiguration()))
                .withSideInput(SCHEMA_SIDE_INPUT_TAG, avroSchema()))
        .setCoder(ProtoCoder.of(InspectionReport.class));
  }

  @AutoValue
  abstract static class ReportUnificationFn
      extends DoFn<Iterable<ColumnInformation>, InspectionReport> {

    static ReportUnificationFn create(
        SourceType sourceType,
        String inputPattern,
        Clock clock,
        JdbcConfiguration jdbcConfiguration) {
      return new AutoValue_MakeInspectionReport_ReportUnificationFn(
          sourceType, inputPattern, clock, jdbcConfiguration);
    }

    abstract SourceType sourceType();

    abstract String inputPattern();

    abstract Clock clock();

    abstract @Nullable JdbcConfiguration jdbcConfiguration();

    @ProcessElement
    public void createCombinedReport(
        @Element Iterable<ColumnInformation> sensitiveColumns,
        @SideInput(SCHEMA_SIDE_INPUT_TAG) String avroSchemaString,
        OutputReceiver<InspectionReport> outputReceiver) {

      var inspectionReportBuilder =
          InspectionReport.newBuilder()
              .setTimestamp(Timestamps.fromMillis(clock().instant().toEpochMilli()))
              .setSourceType(sourceType())
              .setInputPattern(inputPattern())
              .setAvroSchema(avroSchemaString)
              .addAllColumnReport(sensitiveColumns);

      if (jdbcConfiguration() != null) {
        inspectionReportBuilder.setJdbcConfiguration(jdbcConfiguration());
      }

      outputReceiver.output(inspectionReportBuilder.build());
    }
  }
}
