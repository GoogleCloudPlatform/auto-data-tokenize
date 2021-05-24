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
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.auto.value.AutoValue;
import com.google.cloud.datacatalog.v1.ColumnSchema;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.CatalogSchemaWithAvroMapping;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utility Class to convert an Avro schema to DataCatalog Schema for creating Entity in DataCatalog.
 */
public final class AvroSchemaToCatalogSchema {

  public static final String AVRO_SCHEMA_ROOT = "$";
  public static final String CATALOG_SCHEMA_ROOT = "";

  public static final char SCHEMA_FIELD_SEPARATOR = '.';

  public static CatalogSchemaWithAvroMapping convertToCatalogSchemaMapping(String avroSchema) {
    return convertToCatalogSchemaMapping(new Schema.Parser().parse(avroSchema));
  }

  public static CatalogSchemaWithAvroMapping convertToCatalogSchemaMapping(Schema avroSchema) {

    var catalogSchemaAndFlatKeys = SchemaProcessor.forRootElement(avroSchema).process();

    var catalogSchema =
        com.google.cloud.datacatalog.v1.Schema.newBuilder()
            .addAllColumns(catalogSchemaAndFlatKeys.columnSchema().getSubcolumnsList())
            .build();

    return CatalogSchemaWithAvroMapping.newBuilder()
        .setAvroSchemaString(avroSchema.toString())
        .setCatalogSchema(catalogSchema)
        .putAllFlatSchemaKeyMapping(catalogSchemaAndFlatKeys.flatKeys())
        .build();
  }

  /** Process an Avro Field Schema to create ColumnSchema. */
  @AutoValue
  abstract static class SchemaProcessor {

    abstract Schema schema();

    abstract @Nullable String baseDescription();

    abstract String avroParentFlatKey();

    abstract String catalogParentFlatKey();

    abstract boolean ignoreTopRecordNameKey();

    Joiner descriptionJoiner = Joiner.on(' ').skipNulls();
    Joiner flatKeyJoiner = Joiner.on(SCHEMA_FIELD_SEPARATOR).skipNulls();

    @AutoValue.Builder
    abstract static class Builder {

      public abstract Builder setSchema(Schema value);

      public abstract Builder setBaseDescription(String value);

      public abstract Builder setAvroParentFlatKey(String value);

      public abstract Builder setCatalogParentFlatKey(String value);

      public abstract Builder setIgnoreTopRecordNameKey(boolean value);

      public abstract SchemaProcessor build();
    }

    static Builder builder() {
      return new AutoValue_AvroSchemaToCatalogSchema_SchemaProcessor.Builder()
          .setIgnoreTopRecordNameKey(false);
    }

    static SchemaProcessor forRootElement(Schema schema) {
      return builder()
          .setSchema(schema)
          .setAvroParentFlatKey(AVRO_SCHEMA_ROOT)
          .setCatalogParentFlatKey(CATALOG_SCHEMA_ROOT)
          .setBaseDescription(schema.getDoc())
          .setIgnoreTopRecordNameKey(true)
          .build();
    }

    public ColumnSchemaFlatKeys process() {

      switch (schema().getType()) {
        case RECORD:
          return processRecord();

        case ARRAY:
          return processArray();

        case UNION:
          return processUnion();

        case ENUM:
          return ColumnSchemaFlatKeys.of(
              schemaBuilder()
                  .setDescription(
                      descriptionJoiner.join(
                          emptyToNull(baseDescription()),
                          schema().getDoc(),
                          "Permitted Values:",
                          schema().getEnumSymbols()))
                  .build(),
              ImmutableMap.of(avroParentFlatKey(), catalogParentFlatKey()));

        case FIXED:
          return ColumnSchemaFlatKeys.of(
              schemaBuilder()
                  .setDescription(
                      descriptionJoiner.join(
                          emptyToNull(baseDescription()),
                          schema().getDoc(),
                          "Fixed bytes size: " + schema().getFixedSize()))
                  .build(),
              ImmutableMap.of(avroParentFlatKey(), catalogParentFlatKey()));

        case MAP:
          throw new IllegalArgumentException(
              "Type not supported in Schema - " + schema().getType());

          // Simple Fields need no other processing.
        case NULL:
        case STRING:
        case BYTES:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        default:
          return ColumnSchemaFlatKeys.of(
              schemaBuilder().build(),
              ImmutableMap.of(avroParentFlatKey(), catalogParentFlatKey()));
      }
    }

    private ColumnSchemaFlatKeys processRecord() {
      var fieldSchemaAndKeys = extractRecordFields();

      var flatKeys =
          fieldSchemaAndKeys.stream()
              .flatMap(ColumnSchemaFlatKeys::flatKeyEntriesStream)
              .distinct()
              .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

      var colSchemas =
          fieldSchemaAndKeys.stream()
              .map(ColumnSchemaFlatKeys::columnSchema)
              .collect(toImmutableList());

      var recordSchema = schemaBuilder().addAllSubcolumns(colSchemas).build();
      return ColumnSchemaFlatKeys.of(recordSchema, flatKeys);
    }

    private ColumnSchemaFlatKeys processArray() {

      var arrayTypeDetails =
          SchemaProcessor.builder()
              .setSchema(schema().getElementType())
              .setAvroParentFlatKey(avroParentFlatKey())
              .setCatalogParentFlatKey(catalogParentFlatKey())
              .setBaseDescription(descriptionJoiner.join(baseDescription(), schema().getDoc()))
              .setIgnoreTopRecordNameKey(true)
              .build()
              .process();

      var arraySchema =
          schemaBuilder().mergeFrom(arrayTypeDetails.columnSchema()).setMode("REPEATED").build();

      return ColumnSchemaFlatKeys.of(arraySchema, arrayTypeDetails.flatKeys());
    }

    private ColumnSchemaFlatKeys processUnion() {
      var unionTypes = schema().getTypes();
      checkArgument(
          unionTypes.size() == 2 && unionTypes.get(0).getType().equals(Schema.Type.NULL),
          "Only nullable union with one type is supported. found " + unionTypes);

      var unionSchemaPair =
          SchemaProcessor.builder()
              .setSchema(unionTypes.get(1))
              .setAvroParentFlatKey(avroParentFlatKey())
              .setCatalogParentFlatKey(catalogParentFlatKey())
              .setIgnoreTopRecordNameKey(true)
              .build()
              .process();

      var unionSchema =
          schemaBuilder().mergeFrom(unionSchemaPair.columnSchema()).setMode("NULLABLE").build();

      return ColumnSchemaFlatKeys.of(unionSchema, unionSchemaPair.flatKeys());
    }

    private ColumnSchema.Builder schemaBuilder() {
      var builder =
          ColumnSchema.newBuilder()
              .setColumn(schema().getName())
              .setType(schema().getType().toString())
              .setMode(schema().getType().equals(Type.NULL) ? "NULLABLE" : "REQUIRED");

      return baseDescription() == null ? builder : builder.setDescription(baseDescription());
    }

    private ImmutableList<ColumnSchemaFlatKeys> extractRecordFields() {
      return schema().getFields().stream()
          .map(FieldMapper::new)
          .map(FieldMapper::convertField)
          .collect(toImmutableList());
    }

    private class FieldMapper {

      private final Field field;
      private final SchemaProcessor fieldSchemaProcessor;

      private FieldMapper(Field field) {
        this.field = field;
        this.fieldSchemaProcessor =
            SchemaProcessor.builder()
                .setSchema(field.schema())
                .setAvroParentFlatKey(buildAvroFieldFlatKey())
                .setCatalogParentFlatKey(buildCatalogFieldFlatKey())
                .setBaseDescription(descriptionJoiner.join(field.doc(), extractDefaultValue(field)))
                .build();
      }

      private String buildAvroFieldFlatKey() {

        return flatKeyJoiner.join(
            emptyToNull(avroParentFlatKey()), schema().getName(), field.name());
      }

      private String buildCatalogFieldFlatKey() {
        return flatKeyJoiner.join(
            emptyToNull(catalogParentFlatKey()),
            ignoreTopRecordNameKey() ? null : schema().getName(),
            field.name());
      }

      private String extractDefaultValue(Field field) {

        if (field.defaultVal() == null) {
          return null;
        }

        return descriptionJoiner.join(
            "Default Value:",
            field.defaultVal().equals(JsonProperties.NULL_VALUE)
                ? "NULL"
                : field.defaultVal().toString());
      }

      public ColumnSchemaFlatKeys convertField() {
        var convertedSchema = fieldSchemaProcessor.process();

        var fieldSchema =
            ColumnSchema.newBuilder()
                .mergeFrom(convertedSchema.columnSchema())
                .setColumn(field.name())
                .build();

        return ColumnSchemaFlatKeys.of(fieldSchema, convertedSchema.flatKeys());
      }
    }
  }

  @AutoValue
  abstract static class ColumnSchemaFlatKeys {

    private static ColumnSchemaFlatKeys of(
        ColumnSchema columnSchema, ImmutableMap<String, String> flatKeys) {
      return new AutoValue_AvroSchemaToCatalogSchema_ColumnSchemaFlatKeys(columnSchema, flatKeys);
    }

    abstract ColumnSchema columnSchema();

    abstract ImmutableMap<String, String> flatKeys();

    final Stream<Map.Entry<String, String>> flatKeyEntriesStream() {
      return flatKeys().entrySet().stream();
    }
  }

  /** Utility Class, should not be instantiated. */
  private AvroSchemaToCatalogSchema() {}
}
