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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import java.time.Clock;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BaseTypeBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.NamespacedBuilder;
import org.apache.avro.SchemaBuilder.PropBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.SchemaBuilder.UnionAccumulator;

/**
 * Generates an AVRO schema by replacing de-identified column's schema to string and prefixing
 * "encrypted_" to the field name.
 *
 * <p>Does not support converting Map field in the input schema.
 */
public final class DeIdentifiedRecordSchemaConverter {

  public static final String ROOT_SYMBOL = "$";
  public static final String ORIGINAL_SCHEMA_PROPERTY_KEY = "ORIGINAL_TYPE";
  public static final String DEIDENTIFIED_FIELD_NAME_PREFIX = "encrypted_";
  public static final ImmutableSet<Type> COMPLEX_TYPES =
      ImmutableSet.of(Type.ARRAY, Type.RECORD, Type.UNION, Type.ENUM, Type.FIXED);

  private final Schema originalSchema;
  private final ImmutableSet<String> encryptColumnSchemaKeys;
  private final Clock clock;

  /**
   * Instantiate the Schema converter.
   *
   * @param originalSchema the original AVRO schema.
   * @param deIdentifiableColumns the list of column
   */
  private DeIdentifiedRecordSchemaConverter(
      Schema originalSchema, ImmutableSet<String> deIdentifiableColumns, Clock clock) {
    this.originalSchema = originalSchema;
    this.encryptColumnSchemaKeys = deIdentifiableColumns;
    this.clock = clock;
  }

  /**
   * Convenience static factory method to instantiate a Schema converter with the original schema.
   */
  public static DeIdentifiedRecordSchemaConverter withOriginalSchema(Schema schema) {
    return new Builder().setOriginalSchema(schema).build();
  }

  public static DeIdentifiedRecordSchemaConverter withOriginalSchemaJson(String schema) {
    return withOriginalSchema(new Schema.Parser().parse(schema));
  }

  public DeIdentifiedRecordSchemaConverter withEncryptColumnKeys(Collection<String> schemaKeys) {
    return toBuilder().setEncryptColumnNames(schemaKeys).build();
  }

  @VisibleForTesting
  DeIdentifiedRecordSchemaConverter withClock(Clock clock) {
    return toBuilder().setClock(clock).build();
  }

  /**
   * Returns the updated schema with de-identified fields converted to string.
   */
  public Schema updatedSchema() {
    checkState(
        originalSchema != null && encryptColumnSchemaKeys != null && !encryptColumnSchemaKeys
            .isEmpty(),
        "schema and encrypt columns can't be null or empty");

    String updatedRecordDoc =
        String.format(
            "%s%n%s %s",
            originalSchema.getDoc(),
            "Updated for encryption at",
            DateTimeFormatter.ISO_INSTANT.format(clock.instant()));
    return makeRecord(
        recordBuilder(originalSchema).doc(updatedRecordDoc), originalSchema, ROOT_SYMBOL);
  }

  private <T> T makeRecord(RecordBuilder<T> recordBuilder, Schema recordSchema, String parentKey) {
    String recordKey = Joiner.on(".").join(parentKey, recordSchema.getFullName());
    SchemaBuilder.FieldAssembler<T> recordFieldsBuilder = recordBuilder.fields();

    for (Field oldField : recordSchema.getFields()) {
      recordFieldsBuilder =
          new FieldMakerBuilder<>(recordFieldsBuilder)
              .setField(oldField)
              .setParentSchemaKey(recordKey)
              .build()
              .makeField();
    }

    return recordFieldsBuilder.endRecord();
  }

  private static RecordBuilder<Schema> recordBuilder(Schema recordSchema) {
    return addComplexTypeProperties(SchemaBuilder.record(recordSchema.getName()), recordSchema);
  }

  private <T> T makeArray(
      SchemaBuilder.ArrayBuilder<T> arrayBuilder,
      Schema arraySchema,
      String parentKey,
      boolean encrypted) {

    Schema elementType = arraySchema.getElementType();
    return arrayBuilder.items(makeType(elementType, parentKey, encrypted));
  }

  /**
   * Helper build class for FieldMaker class that creates a Field.
   */
  private class FieldMakerBuilder<T> {

    private final FieldAssembler<T> fieldAssembler;
    private Field field;
    private String parentSchemaKey;

    public FieldMakerBuilder(FieldAssembler<T> fieldAssembler) {
      this.fieldAssembler = fieldAssembler;
    }

    public FieldMakerBuilder<T> setField(Field field) {
      this.field = field;
      return this;
    }

    public FieldMakerBuilder<T> setParentSchemaKey(String parentSchemaKey) {
      this.parentSchemaKey = parentSchemaKey;
      return this;
    }

    public FieldMaker<T> build() {
      String fieldSchemaKey = Joiner.on(".").join(parentSchemaKey, field.name());
      return new FieldMaker<>(
          fieldAssembler, field, fieldSchemaKey, encryptColumnSchemaKeys.contains(fieldSchemaKey));
    }
  }

  /**
   * Helper class to create a field of an AVRO Record type.
   */
  private class FieldMaker<T> {

    private final FieldAssembler<T> parentFieldAssembler;
    private final Field field;
    private final String fieldSchemaKey;
    private final boolean deIdentifiedField;

    private FieldMaker(
        FieldAssembler<T> parentFieldAssembler,
        Field field,
        String fieldSchemaKey,
        boolean deIdentifiedField) {
      this.parentFieldAssembler = parentFieldAssembler;
      this.field = field;
      this.fieldSchemaKey = fieldSchemaKey;
      this.deIdentifiedField = deIdentifiedField;
    }

    private FieldAssembler<T> makeField() {
      FieldBuilder<T> fieldBuilder = makeFieldBuilder();

      SchemaBuilder.GenericDefault<T> builder =
          fieldBuilder.type(makeType(field.schema(), fieldSchemaKey, deIdentifiedField));

      if (field.defaultVal() == null) {
        return builder.noDefault();
      } else if (field.defaultVal() instanceof JsonProperties.Null) {
        return builder.withDefault(null);
      }

      return builder.withDefault(field.defaultVal());
    }

    private FieldBuilder<T> makeFieldBuilder() {
      FieldBuilder<T> builder = addFieldProperties(parentFieldAssembler.name(updatedFieldName()));

      if (deIdentifiedField) {
        builder.prop(ORIGINAL_SCHEMA_PROPERTY_KEY, field.schema().toString());
      }

      return builder;
    }

    private String updatedFieldName() {
      return (deIdentifiedField ? DEIDENTIFIED_FIELD_NAME_PREFIX : "") + field.name();
    }

    private FieldBuilder<T> addFieldProperties(FieldBuilder<T> builder) {
      return makeTypeProps(builder, field)
          .doc(field.schema().getDoc())
          .aliases(extractAliases(field.aliases()));
    }
  }

  private Schema makeType(Schema fieldSchema, String fieldSchemaKey, boolean encryptedField) {
    switch (fieldSchema.getType()) {
      case ARRAY:
        SchemaBuilder.TypeBuilder<Schema> arrayTypeBuilder = SchemaBuilder.array().items();
        Schema updatedType = makeType(fieldSchema.getElementType(), fieldSchemaKey, encryptedField);
        return arrayTypeBuilder.type(updatedType);

      case UNION:
        BaseTypeBuilder<UnionAccumulator<Schema>> unionBuilder = SchemaBuilder.unionOf();

        List<Schema> unionTypes = fieldSchema.getTypes();

        if (unionTypes.size() > 2 || !unionTypes.get(0).getType().equals(Type.NULL)) {
          throw new UnsupportedOperationException(
              "Union can contain max of two types. with first being null");
        }

        int unionListLastIndex = unionTypes.size() - 1;
        UnionAccumulator<Schema> unionAccumulator = null;

        for (int typeIndex = 0; typeIndex <= unionListLastIndex; typeIndex++) {
          unionAccumulator =
              makeUnionType(
                  unionBuilder, unionTypes.get(typeIndex), fieldSchemaKey, encryptedField);

          if (typeIndex < unionListLastIndex) {
            unionBuilder = unionAccumulator.and();
          }
        }

        return checkNotNull(unionAccumulator).endUnion();

      case RECORD:
        return makeRecord(recordBuilder(fieldSchema), fieldSchema, fieldSchemaKey);

      case ENUM:
      case FIXED:
      case STRING:
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return (encryptedField) ? Schema.create(Type.STRING) : fieldSchema;
      default:
      case NULL:
      case MAP:
        throw new UnsupportedOperationException(
            "Type not supported in Schema - " + fieldSchema.getType());
    }
  }

  private static String[] extractAliases(Set<String> aliases) {
    checkNotNull(aliases);
    return aliases.toArray(new String[0]);
  }

  private UnionAccumulator<Schema> makeUnionType(
      BaseTypeBuilder<UnionAccumulator<Schema>> unionBuilder,
      Schema type,
      String parentSchemaKey,
      boolean encryptedField) {

    if (!type.getType().equals(Type.NULL)
        && !COMPLEX_TYPES.contains(type.getType())
        && encryptedField) {
      return makeTypeProps(unionBuilder.stringBuilder(), type).endString();
    }

    switch (type.getType()) {
      case RECORD:
        return makeRecord(
            addComplexTypeProperties(unionBuilder.record(type.getName()), type),
            type,
            parentSchemaKey);
      case ENUM:
        return (encryptedField)
            ? makeTypeProps(unionBuilder.stringBuilder(), type).endString()
            : addComplexTypeProperties(unionBuilder.enumeration(type.getName()), type)
                .defaultSymbol(type.getEnumDefault())
                .symbols(type.getEnumSymbols().toArray(new String[0]));
      case ARRAY:
        return makeArray(unionBuilder.array(), type, parentSchemaKey, encryptedField);
      case FIXED:
        return (encryptedField)
            ? makeTypeProps(unionBuilder.stringBuilder(), type).endString()
            : addComplexTypeProperties(unionBuilder.fixed(type.getName()), type)
                .size(type.getFixedSize());
      case STRING:
        return makeTypeProps(unionBuilder.stringBuilder(), type).endString();
      case BYTES:
        return makeTypeProps(unionBuilder.bytesBuilder(), type).endBytes();
      case INT:
        return makeTypeProps(unionBuilder.intBuilder(), type).endInt();
      case LONG:
        return makeTypeProps(unionBuilder.longBuilder(), type).endLong();
      case FLOAT:
        return makeTypeProps(unionBuilder.floatBuilder(), type).endFloat();
      case DOUBLE:
        return makeTypeProps(unionBuilder.doubleBuilder(), type).endDouble();
      case BOOLEAN:
        return makeTypeProps(unionBuilder.booleanBuilder(), type).endBoolean();
      case NULL:
        return unionBuilder.nullType();
      default:
        throw new UnsupportedOperationException("Union of Union/Map is invalid schema");
    }
  }

  private static <R extends PropBuilder<R>> R makeTypeProps(R builder, JsonProperties type) {
    if (type.hasProps()) {
      type.getObjectProps().forEach(builder::prop);
    }

    return builder;
  }

  private static <T, R extends NamespacedBuilder<T, R>> R addComplexTypeProperties(
      R builder, Schema type) {
    return makeTypeProps(builder, type)
        .namespace(type.getNamespace())
        .aliases(extractAliases(type.getAliases()))
        .doc(type.getDoc());
  }


  private Builder toBuilder() {
    return new Builder(originalSchema, encryptColumnSchemaKeys, clock);
  }


  /**
   * Builder for the DeIdentifiedRecordSchemaCreator.
   */
  public static class Builder {

    private Schema originalSchema;
    private ImmutableSet<String> encryptColumns;
    private Clock clock;

    Builder() {
    }

    Builder(Schema originalSchema, ImmutableSet<String> encryptColumns, Clock clock) {
      this.originalSchema = originalSchema;
      this.encryptColumns = encryptColumns;
      this.clock = clock;
    }

    Builder setOriginalSchema(Schema originalSchema) {
      this.originalSchema = checkNotNull(originalSchema, "original schema can't be null");
      return this;
    }

    Builder setEncryptColumnNames(Collection<String> encryptColumns) {
      this.encryptColumns = ImmutableSet.copyOf(checkNotNull(encryptColumns));
      return this;
    }

    Builder setClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    DeIdentifiedRecordSchemaConverter build() {
      return new DeIdentifiedRecordSchemaConverter(
          originalSchema, encryptColumns, firstNonNull(clock, Clock.systemUTC()));
    }
  }

  /**
   * Uses the clock to append the updated timestamp in the output schema.
   */
  @VisibleForTesting
  Clock getClock() {
    return clock;
  }
}
