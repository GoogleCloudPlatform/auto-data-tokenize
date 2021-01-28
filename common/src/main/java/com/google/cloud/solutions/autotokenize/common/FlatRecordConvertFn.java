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

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

/**
 * Flattens a GenericRecord and separates the Avro schema as a KV.
 */
public interface FlatRecordConvertFn<T> extends SerializableFunction<T, KV<FlatRecord, String>> {


  final class GenericRecordFlatteningFn implements FlatRecordConvertFn<GenericRecord> {

    private static GenericRecordFlatteningFn genericRecordFlatteningFn = new GenericRecordFlatteningFn();

    @Override
    public KV<FlatRecord, String> apply(GenericRecord record) {
      return KV.of(RecordFlattener.forGenericRecord().flatten(record), record.getSchema().toString());
    }

    private GenericRecordFlatteningFn() {}
  }

  final class TableRowFlatteningFn implements FlatRecordConvertFn<SchemaAndRecord> {

    private static TableRowFlatteningFn tableRowFlatteningFn = new TableRowFlatteningFn();

    @Override
    public KV<FlatRecord, String> apply(SchemaAndRecord input) {
      return KV.of(RecordFlattener.forGenericRecord().flatten(input.getRecord()), input.getRecord().getSchema().toString());
    }

    private TableRowFlatteningFn() {}
  }

  static GenericRecordFlatteningFn forGenericRecord() {
    return GenericRecordFlatteningFn.genericRecordFlatteningFn;
  }

  static TableRowFlatteningFn forBigQueryTableRow() { return TableRowFlatteningFn.tableRowFlatteningFn;}
}
