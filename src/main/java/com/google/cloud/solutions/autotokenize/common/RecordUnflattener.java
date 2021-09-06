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

import static com.google.cloud.solutions.autotokenize.common.JsonConvertor.convertJsonToAvro;

import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.api.client.util.Maps;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.privacy.dlp.v2.Value;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Converts a {@link FlatRecord} into AVRO Record by converting the flat-record to a JSON and using
 * the {@link JsonUnflattener} to convert the flat Json into nested Json object.
 */
public class RecordUnflattener {

  private final Schema schema;

  private RecordUnflattener(Schema schema) {
    this.schema = schema;
  }

  /**
   * Instantiates an Unflattener for the given output schema.
   *
   * @param schema the AVRO Schema to convert the flat-record to.
   */
  public static RecordUnflattener forSchema(Schema schema) {
    return new RecordUnflattener(schema);
  }

  /** Returns an AVRO record by unflattening the FlatRecord through JSON unflattener. */
  public GenericRecord unflatten(FlatRecord flatRecord) {

    Map<String, Object> jsonValueMap = Maps.newHashMap();

    for (Map.Entry<String, Value> entry : flatRecord.getValuesMap().entrySet()) {
      var valueProcessor = new ValueProcessor(entry);
      jsonValueMap.put(valueProcessor.cleanKey(), valueProcessor.convertedValue());
    }

    String unflattenedRecordJson = new JsonUnflattener(jsonValueMap).unflatten();

    return convertJsonToAvro(schema, unflattenedRecordJson);
  }

  /** Helper class to convert a {@link Value} to JSON compatible object. */
  private static class ValueProcessor {

    /** REGEX pattern to extract a string value's actual type that is suffixed with the key name. */
    private static final Pattern VALUE_PATTERN = Pattern.compile("/(?<type>\\w+)$");

    private final String rawKey;
    private final Value value;

    private ValueProcessor(Map.Entry<String, Value> entry) {
      this.rawKey = entry.getKey();
      this.value = entry.getValue();
    }

    /**
     * Converts a {@link Value} object to Schema appropriate Java object.
     *
     * @return Java Object equivalent for the Value Object.
     */
    private Object convertedValue() {
      String keyType = keyType();

      switch (value.getTypeCase()) {
        case INTEGER_VALUE:
          return new BigInteger(String.valueOf(value.getIntegerValue()));
        case FLOAT_VALUE:
          return new BigDecimal(String.valueOf(value.getFloatValue()));

        case BOOLEAN_VALUE:
          return value.getBooleanValue();

        case TYPE_NOT_SET:
          return null;
        default:
        case STRING_VALUE:
          if (keyType != null && keyType.equals("bytes")) {
            return ByteValueConverter.of(value).asJsonString();
          }
          return value.getStringValue();
      }
    }

    /** Returns the original type of a string value. */
    private String keyType() {
      var matcher = VALUE_PATTERN.matcher(rawKey);
      if (matcher.find()) {
        return matcher.group("type");
      }

      return null;
    }

    /** Remove the type-suffix from string value's key. */
    private String cleanKey() {
      return VALUE_PATTERN.matcher(rawKey).replaceFirst("").replaceFirst("^\\$\\.", "");
    }
  }
}
