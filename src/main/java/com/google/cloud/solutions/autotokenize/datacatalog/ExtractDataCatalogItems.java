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
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.JdbcConfiguration;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.UpdatableDataCatalogItems;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import com.google.protobuf.util.Timestamps;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoValue
public abstract class ExtractDataCatalogItems
    extends PTransform<PCollection<ColumnInformation>, PCollection<UpdatableDataCatalogItems>> {

  private static final String SCHEMA_SIDE_INPUT_TAG = "schemaTag";

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  abstract SourceType sourceType();

  abstract String inputPattern();

  abstract PCollectionView<String> schema();

  abstract String inspectionTagTemplateId();

  abstract @Nullable JdbcConfiguration jdbcConfiguration();

  abstract Clock clock();

  public static Builder builder() {
    return new AutoValue_ExtractDataCatalogItems.Builder().setClock(Clock.systemUTC());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSourceType(SourceType value);

    public abstract Builder setInputPattern(String value);

    public abstract Builder setSchema(PCollectionView<String> value);

    public abstract Builder setInspectionTagTemplateId(String value);

    public abstract Builder setJdbcConfiguration(JdbcConfiguration value);

    @VisibleForTesting
    abstract Builder setClock(Clock clock);

    abstract ExtractDataCatalogItems autoBuild();

    public ExtractDataCatalogItems build() {
      var transform = autoBuild();

      checkArgument(
          !transform.sourceType().equals(SourceType.JDBC_TABLE)
              || transform.jdbcConfiguration() != null,
          "Provide JDBC Configuration");

      checkArgument(
          transform
              .inspectionTagTemplateId()
              .matches("^projects/[\\w:\\.\\-]+/locations/[\\w\\-]+/tagTemplates/[\\w]+$"),
          "inspectionTagTemplateId incorrect format.");

      return transform;
    }
  }

  @Override
  public PCollection<UpdatableDataCatalogItems> expand(
      PCollection<ColumnInformation> sensitiveColumns) {
    return sensitiveColumns
        .apply(WithKeys.of("const_key"))
        .apply(GroupByKey.create())
        .apply(Values.create())
        .apply(
            ParDo.of(
                    DataCatalogTagExtractorFn.create(
                        sourceType(),
                        inputPattern(),
                        clock(),
                        inspectionTagTemplateId(),
                        jdbcConfiguration()))
                .withSideInput(SCHEMA_SIDE_INPUT_TAG, schema()));
  }

  @AutoValue
  abstract static class DataCatalogTagExtractorFn
      extends DoFn<Iterable<ColumnInformation>, UpdatableDataCatalogItems> {

    public static DataCatalogTagExtractorFn create(
        SourceType sourceType,
        String inputPattern,
        Clock clock,
        String inspectionTagTemplateId,
        JdbcConfiguration jdbcConfiguration) {
      return new AutoValue_ExtractDataCatalogItems_DataCatalogTagExtractorFn(
          sourceType, inputPattern, clock, inspectionTagTemplateId, jdbcConfiguration);
    }

    abstract SourceType sourceType();

    abstract String inputPattern();

    abstract Clock clock();

    abstract String inspectionTagTemplateId();

    abstract @Nullable JdbcConfiguration jdbcConfiguration();

    @ProcessElement
    public void processColumns(
        @Element Iterable<ColumnInformation> sensitiveColumns,
        @SideInput(SCHEMA_SIDE_INPUT_TAG) String avroSchemaString,
        OutputReceiver<UpdatableDataCatalogItems> outputReceiver) {

      var catalogSchemaAndFlatKeys = convertToCatalogSchemaMapping(avroSchemaString);

      var entry =
          new EntryCreator(avroSchemaString, catalogSchemaAndFlatKeys.getCatalogSchema())
              .buildEntry();

      var tags =
          new SensitiveTagsCreator(
                  sensitiveColumns, catalogSchemaAndFlatKeys.getFlatSchemaKeyMappingMap())
              .buildTags();

      if (entry.isPresent() || (tags.isPresent() && !tags.get().isEmpty())) {

        var updatableItemsBuilder =
            UpdatableDataCatalogItems.newBuilder()
                .setSourceType(sourceType())
                .setInputPattern(inputPattern());
        entry.ifPresent(updatableItemsBuilder::setEntry);
        tags.ifPresent(updatableItemsBuilder::addAllTags);

        outputReceiver.output(updatableItemsBuilder.build());
      }
    }

    /** Helper class to create new DataCatalog Entry for source type. */
    private class EntryCreator {

      private final Schema avroSchema;
      private final com.google.cloud.datacatalog.v1.Schema dataCatalogSchema;

      public EntryCreator(
          String avroSchemaString, com.google.cloud.datacatalog.v1.Schema dataCatalogSchema) {
        this.avroSchema = new Schema.Parser().parse(avroSchemaString);
        this.dataCatalogSchema = dataCatalogSchema;
      }

      private Optional<Entry> buildEntry() {
        switch (sourceType()) {
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
            logger.atSevere().log("Skipping DataCatalog Tags: SourceType (%s).", sourceType());
            return Optional.empty();
        }
      }

      private Entry fileSetEntry() {
        return entryBuilder()
            .setType(EntryType.FILESET)
            .setGcsFilesetSpec(GcsFilesetSpec.newBuilder().addFilePatterns(inputPattern()).build())
            .build();
      }

      private Entry jdbcEntry() {
        return entryBuilder()
            .setUserSpecifiedType("Database")
            .setUserSpecifiedSystem(
                jdbcConfiguration().getDriverClassName().replaceAll("[^\\w]", "_"))
            .setDescription(
                Joiner.on(' ')
                    .join(
                        "SQL Database details:\n",
                        "JDBC Connection URL: ",
                        jdbcConfiguration().getConnectionUrl(),
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

      private final ImmutableList<ColumnInformation> sensitiveColumns;
      private final ImmutableMap<String, String> avroToCatalogSchemaMap;

      public SensitiveTagsCreator(
          Iterable<ColumnInformation> sensitiveColumns,
          Map<String, String> avroToCatalogSchemaMap) {
        this.sensitiveColumns = ImmutableList.copyOf(sensitiveColumns);
        this.avroToCatalogSchemaMap = ImmutableMap.copyOf(avroToCatalogSchemaMap);
      }

      private Optional<ImmutableList<Tag>> buildTags() {

        if (sourceType().equals(SourceType.BIGQUERY_QUERY)) {
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
            .setTemplate(inspectionTagTemplateId())
            .putFields(
                "infoTypes",
                TagField.newBuilder().setStringValue(infoTypesDetected.toString()).build())
            .putFields(
                "inspectTimestamp",
                TagField.newBuilder().setTimestampValue(getCurrentTimestamp()).build())
            .build();
      }

      private com.google.protobuf.Timestamp getCurrentTimestamp() {
        return Timestamps.fromMillis(Instant.now(clock()).toEpochMilli());
      }
    }
  }
}
