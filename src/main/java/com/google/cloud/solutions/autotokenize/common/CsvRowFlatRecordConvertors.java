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

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.common.CsvIO.CsvRow;
import com.google.privacy.dlp.v2.Value;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

/** Utility functions to convert {@link FlatRecord} elements to {@link CsvRow} and reverse. */
public final class CsvRowFlatRecordConvertors {

  private static final String AVRO_ROOT_RECORD_NAME = "CsvRecord";

  public static CsvRowToFlatRecordFn csvRowToFlatRecordFn() {
    return new CsvRowToFlatRecordFn();
  }

  public static FlatRecordToCsvRowFn flatRecordToCsvRowFn() {
    return new FlatRecordToCsvRowFn();
  }

  public static CsvRowToFlatRecordAndSchemaFn csvRowToFlatRecordAndSchemaFn() {
    return new CsvRowToFlatRecordAndSchemaFn();
  }

  public static class CsvRowToFlatRecordAndSchemaFn
      extends SimpleFunction<CsvRow, KV<FlatRecord, String>> {

    CsvRowToFlatRecordFn flattenFn = csvRowToFlatRecordFn();

    @Override
    public KV<FlatRecord, String> apply(CsvRow input) {
      return KV.of(
          flattenFn.apply(input), makeCsvAvroSchema(input.headersIterable()).toString(true));
    }
  }

  public static class CsvRowToFlatRecordFn extends SimpleFunction<CsvRow, FlatRecord> {

    @Override
    public FlatRecord apply(CsvRow input) {

      var flatRecordBuilder = FlatRecord.newBuilder();

      input.headers().entrySet().stream()
          .sorted(Comparator.comparingInt(Map.Entry::getKey))
          .forEach(
              indexAndName -> {
                var flatKey = "$." + indexAndName.getValue();
                var schemaKey =
                    String.format("$.%s.%s", AVRO_ROOT_RECORD_NAME, indexAndName.getValue());
                var value =
                    Value.newBuilder().setStringValue(input.get(indexAndName.getKey())).build();
                flatRecordBuilder.putFlatKeySchema(flatKey, schemaKey);
                flatRecordBuilder.putValues(flatKey, value);
              });

      return flatRecordBuilder.build();
    }
  }

  public static class FlatRecordToCsvRowFn extends SimpleFunction<FlatRecord, CsvRow> {

    @Override
    public CsvRow apply(FlatRecord input) {

      var indexMap = new HashMap<Integer, String>();
      var valuesMap = new HashMap<String, String>();

      int index = 0;
      for (var values : input.getValuesMap().entrySet()) {
        var colName = values.getKey().replaceAll("^\\$\\.", "");
        indexMap.put(index, colName);
        valuesMap.put(colName, values.getValue().getStringValue());
        index++;
      }

      return new CsvRow(valuesMap, indexMap);
    }
  }

  /**
   * Make Avro Schema for CSV headers with String type.
   *
   * @param headers the CSV column names
   * @return AVRO Schema with field names as provided column names of String type and inOrder.
   */
  public static Schema makeCsvAvroSchema(List<String> headers) {
    var fields =
        headers.stream()
            .map(headerName -> new Field(headerName, Schema.create(Type.STRING)))
            .collect(toImmutableList());

    return Schema.createRecord(
        AVRO_ROOT_RECORD_NAME,
        AVRO_ROOT_RECORD_NAME + " created by auto-data-tokenize",
        null,
        false,
        fields);
  }

  private CsvRowFlatRecordConvertors() {}
}
