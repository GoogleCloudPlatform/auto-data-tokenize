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

package com.google.cloud.solutions.autotokenize.datacatalog;

import static com.google.cloud.solutions.autotokenize.common.AvroSchemaToCatalogSchema.convertToCatalogSchemaMapping;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.auto.value.AutoValue;
import com.google.cloud.datacatalog.v1.Entry;
import com.google.cloud.datacatalog.v1.EntryType;
import com.google.cloud.datacatalog.v1.GcsFilesetSpec;
import com.google.cloud.datacatalog.v1.Tag;
import com.google.cloud.datacatalog.v1.TagField;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.InfoTypeInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.InspectionReport;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.UpdatableDataCatalogItems;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class MakeDataCatalogItems
    extends PTransform<PCollection<InspectionReport>, PCollection<UpdatableDataCatalogItems>> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static MakeDataCatalogItems create(String inspectionTagTemplateId) {
    checkArgument(
        inspectionTagTemplateId.matches(
            "^projects/[\\w:\\.\\-]+/locations/[\\w\\-]+/tagTemplates/[\\w]+$"),
        "inspectionTagTemplateId incorrect format.");

    return new AutoValue_MakeDataCatalogItems(inspectionTagTemplateId);
  }

  abstract String inspectionTagTemplateId();

  @Override
  public PCollection<UpdatableDataCatalogItems> expand(
      PCollection<InspectionReport> inspectionReport) {
    return inspectionReport
        .apply(ParDo.of(new DataCatalogTagExtractorFn(inspectionTagTemplateId())))
        .setCoder(ProtoCoder.of(UpdatableDataCatalogItems.class));
  }

  private static final class DataCatalogTagExtractorFn
      extends DoFn<InspectionReport, UpdatableDataCatalogItems> {

    private final String inspectionTagTemplateId;

    public DataCatalogTagExtractorFn(String inspectionTagTemplateId) {
      this.inspectionTagTemplateId = inspectionTagTemplateId;
    }

    @ProcessElement
    public void processColumns(
        @Element InspectionReport inspectionReport,
        OutputReceiver<UpdatableDataCatalogItems> outputReceiver) {

      var catalogSchemaAndFlatKeys =
          convertToCatalogSchemaMapping(inspectionReport.getAvroSchema());

      var entry =
          new EntryCreator(inspectionReport, catalogSchemaAndFlatKeys.getCatalogSchema())
              .buildEntry();

      var tags =
          new SensitiveTagsCreator(
                  inspectionReport, catalogSchemaAndFlatKeys.getFlatSchemaKeyMappingMap())
              .buildTags();

      if (entry.isPresent() || (tags.isPresent() && !tags.get().isEmpty())) {

        var updatableItemsBuilder =
            UpdatableDataCatalogItems.newBuilder()
                .setSourceType(inspectionReport.getSourceType())
                .setInputPattern(inspectionReport.getInputPattern());
        entry.ifPresent(updatableItemsBuilder::setEntry);
        tags.ifPresent(updatableItemsBuilder::addAllTags);

        outputReceiver.output(updatableItemsBuilder.build());
      }
    }

    /** Helper class to create new DataCatalog Entry for source type. */
    private static class EntryCreator {

      private final InspectionReport inspectionReport;
      private final Schema avroSchema;
      private final com.google.cloud.datacatalog.v1.Schema dataCatalogSchema;

      public EntryCreator(
          InspectionReport inspectionReport,
          com.google.cloud.datacatalog.v1.Schema dataCatalogSchema) {
        this.inspectionReport = inspectionReport;
        this.avroSchema = new Schema.Parser().parse(inspectionReport.getAvroSchema());
        this.dataCatalogSchema = dataCatalogSchema;
      }

      private Optional<Entry> buildEntry() {
        switch (inspectionReport.getSourceType()) {
          case AVRO:
          case PARQUET:
            return Optional.of(fileSetEntry());

          case JDBC_TABLE:
            return Optional.of(jdbcEntry());

          case BIGQUERY_TABLE:
          case BIGQUERY_QUERY:
          case UNRECOGNIZED:
          case UNKNOWN_FILE_TYPE:
          default:
            logger.atSevere().log(
                "Skipping DataCatalog Tags: SourceType (%s).", inspectionReport.getSourceType());
            return Optional.empty();
        }
      }

      private Entry fileSetEntry() {
        return entryBuilder()
            .setType(EntryType.FILESET)
            .setGcsFilesetSpec(
                GcsFilesetSpec.newBuilder()
                    .addFilePatterns(inspectionReport.getInputPattern())
                    .build())
            .build();
      }

      private Entry jdbcEntry() {
        return entryBuilder()
            .setUserSpecifiedType("Database")
            .setUserSpecifiedSystem(
                inspectionReport
                    .getJdbcConfiguration()
                    .getDriverClassName()
                    .replaceAll("[^\\w]", "_"))
            .setDescription(
                Joiner.on(' ')
                    .join(
                        "SQL Database details:\n",
                        "JDBC Connection URL: ",
                        inspectionReport.getJdbcConfiguration().getConnectionUrl(),
                        "\n",
                        basicEntryDescription()))
            .build();
      }

      private Entry.Builder entryBuilder() {
        return Entry.newBuilder()
            .setDescription(basicEntryDescription())
            .setSchema(dataCatalogSchema);
      }

      private String basicEntryDescription() {
        return Joiner.on(' ')
            .join(
                "Entry created by Auto Data identification pipeline.",
                "Original Avro Schema:",
                avroSchema.toString());
      }
    }

    private class SensitiveTagsCreator {

      private final InspectionReport inspectionReport;
      private final ImmutableList<ColumnInformation> sensitiveColumns;
      private final ImmutableMap<String, String> avroToCatalogSchemaMap;

      public SensitiveTagsCreator(
          InspectionReport inspectionReport, Map<String, String> avroToCatalogSchemaMap) {
        this.inspectionReport = inspectionReport;
        this.sensitiveColumns = ImmutableList.copyOf(inspectionReport.getColumnReportList());
        this.avroToCatalogSchemaMap = ImmutableMap.copyOf(avroToCatalogSchemaMap);
      }

      private Optional<ImmutableList<Tag>> buildTags() {

        if (inspectionReport.getSourceType().equals(SourceType.BIGQUERY_QUERY)) {
          return Optional.empty();
        }

        return Optional.of(
            sensitiveColumns.stream()
                .map(this::buildTag)
                .filter(Objects::nonNull)
                .collect(toImmutableList()));
      }

      private Tag buildTag(ColumnInformation columnInformation) {
        var infoTypesDetected =
            columnInformation.getInfoTypesList().stream()
                .map(InfoTypeInformation::getInfoType)
                .collect(toImmutableList());

        var mappedCatalogSchemaColumn =
            avroToCatalogSchemaMap.get(columnInformation.getColumnName());

        if (mappedCatalogSchemaColumn == null) {
          return null;
        }

        return Tag.newBuilder()
            .setColumn(mappedCatalogSchemaColumn)
            .setTemplate(inspectionTagTemplateId)
            .putFields(
                "infoTypes",
                TagField.newBuilder().setStringValue(infoTypesDetected.toString()).build())
            .putFields(
                "inspectTimestamp",
                TagField.newBuilder().setTimestampValue(inspectionReport.getTimestamp()).build())
            .build();
      }
    }
  }
}
