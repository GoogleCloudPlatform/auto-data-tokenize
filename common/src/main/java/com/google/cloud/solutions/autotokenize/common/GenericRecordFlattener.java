/*
 * Copyright 2020 Google LLC
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

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.privacy.dlp.v2.Value;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

/**
 * Flattens an AVRO {@link GenericRecord} into a map of flattened JSONPath key and value pairs.
 * Supports record and array type fields, does not support Map field.
 *
 * <p>example:
 * <code>
 * { "name": "John Doe", "contacts": [ { "contact": { "type": "home", "number": "123-456-789" } }, {
 * "contact": { "type": "work", "number": "987-654-321" } }, { "null": null } ] }
 * </code>
 * is converted as
 * <code>
 * { $.name -> {string_value: "John Doe"}, $.contacts[0].contact.type -> {string_value: "home"},
 * $.contacts[0].contact.number -> {string_value: "123-456-789"}, $.contacts[1].contact.type ->
 * {string_value: "work"}, $.contacts[1].contact.number -> {string_value: "987-654-321"} }
 * </code>
 */
public final class GenericRecordFlattener implements RecordFlattener<GenericRecord>{

  public static final String RECORD_ROOT_SYMBOL = "$";

  /**
   * Convenience static factory to instantiate a converter for a Generic Record.
   *
   * @param record the AVRO record to flatten.
   */
  @Override
  public FlatRecord flatten(GenericRecord record) {
    return new TypeFlattener(record).convert();
  }

  /**
   * Helper class to actually flatten an AVRO Record.
   */
  private static final class TypeFlattener {

    private final Schema schema;
    private final GenericRecord record;
    private final Map<String, Value> valueMap;
    private final Map<String, String> flatKeySchemaMap;

    private TypeFlattener(Schema schema, GenericRecord record) {
      this.schema = schema;
      this.record = record;
      this.valueMap = Maps.newHashMap();
      this.flatKeySchemaMap = Maps.newHashMap();
    }

    private TypeFlattener(GenericRecord record) {
      this(record.getSchema(), record);
    }

    /**
     * Returns a field's key name suffixed with "/bytes" to help distinguish the type in unflatten
     * stage.
     */
    private static String makeByteFieldKey(String fieldKey) {
      return fieldKey + "/bytes";
    }

    private FlatRecord convert() {
      convertRecord(record, schema, RECORD_ROOT_SYMBOL, RECORD_ROOT_SYMBOL);
      return FlatRecord.newBuilder()
          .putAllValues(valueMap)
          .putAllFlatKeySchema(flatKeySchemaMap)
          .build();
    }

    /**
     * Flattens the provided value as per the type of item.
     *
     * @param value the object/value to flatten, the type depends on fieldSchema type.
     * @param fieldSchema the schema of the object to be flattened.
     * @param parentKey the flat-field-key of the parent of this field.
     * @param fieldName the name of the field to be flattened.
     * @param schemaKey the flat-key of the schema field.
     */
    private void processType(Object value, Schema fieldSchema, String parentKey, String fieldName,
        String schemaKey) {
      String fieldKey = Joiner.on(".").skipNulls().join(parentKey, fieldName);

      switch (fieldSchema.getType()) {
        case RECORD:

          String recordFieldKey =
              isBlank(fieldName) ? parentKey :
                  String.format("%s.[\"%s\"]", parentKey, fieldName);
          convertRecord((GenericRecord) value, fieldSchema, recordFieldKey, schemaKey);
          break;

        case ARRAY:
          processArray(value, fieldSchema, fieldKey, schemaKey);
          break;

        case UNION:
          processUnion(value, fieldSchema, parentKey, fieldName, schemaKey);
          break;

        case ENUM:
        case STRING:
          putValue(
              fieldKey, schemaKey, Value.newBuilder().setStringValue(value.toString()).build());
          break;

        case BOOLEAN:
          putValue(
              fieldKey, schemaKey, Value.newBuilder().setBooleanValue((boolean) value).build());
          break;

        case FLOAT:
          putValue(fieldKey, schemaKey, Value.newBuilder().setFloatValue((float) value).build());
          break;

        case DOUBLE:
          putValue(fieldKey, schemaKey, Value.newBuilder().setFloatValue((double) value).build());
          break;

        case INT:
          putValue(fieldKey, schemaKey, Value.newBuilder().setIntegerValue((int) value).build());
          break;
        case LONG:
          putValue(fieldKey, schemaKey, Value.newBuilder().setIntegerValue((long) value).build());
          break;

        case FIXED:
          putValue(
              makeByteFieldKey(fieldKey),
              schemaKey,
              ByteValueConverter.convertBytesToValue(((GenericFixed) value).bytes()));
          break;
        case BYTES:
          putValue(
              makeByteFieldKey(fieldKey),
              schemaKey,
              ByteValueConverter.convertBytesToValue(((ByteBuffer) value).array()));
          break;

        case NULL:
          break;
        case MAP:
          throw new UnsupportedOperationException(
              String.format("Unsupported Type MAP at %s", fieldKey));
      }
    }

    private void convertRecord(
        GenericRecord record, Schema fieldSchema, String parentKey, String parentSchemaKey) {

      String recordName = fieldSchema.getFullName();

      for (Field field : fieldSchema.getFields()) {
        String fieldName = field.name();
        Object value = record.get(fieldName);
        String fieldSchemaKey = Joiner.on(".").join(parentSchemaKey, recordName, fieldName);
        processType(value, field.schema(), parentKey, fieldName, fieldSchemaKey);
      }
    }

    private void processArray(Object value, Schema fieldSchema, String fieldKey, String schemaKey) {
      List<?> array = (List<?>) value;
      Schema arrayType = fieldSchema.getElementType();
      for (int index = 0; index < array.size(); index++) {
        processType(array.get(index), arrayType, String.format("%s[%s]", fieldKey, index), null,
            schemaKey);
      }
    }

    private void processUnion(Object value, Schema fieldSchema, String parentKey, String fieldKey,
        String schemaKey) {
      if (value == null) {
        putValue(Joiner.on(".").skipNulls().join(parentKey, fieldKey), schemaKey,
            Value.getDefaultInstance());
        return;
      }

      List<Schema> unionTypes = fieldSchema.getTypes();
      if (unionTypes.size() != 2 || !unionTypes.get(0).getType().equals(Schema.Type.NULL)) {
        throw new UnsupportedOperationException(
            "Only nullable union with one type is supported. found " + unionTypes);
      }

      Schema nonNullType = unionTypes.get(1);
      processType(value, nonNullType, Joiner.on(".").skipNulls().join(parentKey, fieldKey),
          nonNullType.getFullName(), schemaKey);
    }

    private void putValue(String fieldKey, String schemaKey, Value value) {
      valueMap.put(fieldKey, value);
      flatKeySchemaMap.put(fieldKey, schemaKey);
    }
  }
}
