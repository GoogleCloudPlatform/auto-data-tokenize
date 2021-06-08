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
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;

/** Flattens a GenericRecord and separates the Avro schema as a KV. */
public abstract class FlatRecordConvertFn<T> extends SimpleFunction<T, KV<FlatRecord, String>> {

  static final class GenericRecordFlatteningFn extends FlatRecordConvertFn<GenericRecord> {

    private static GenericRecordFlatteningFn genericRecordFlatteningFn =
        new GenericRecordFlatteningFn();

    @Override
    public KV<FlatRecord, String> apply(GenericRecord genericRecord) {
      return KV.of(
          RecordFlattener.forGenericRecord().flatten(genericRecord),
          genericRecord.getSchema().toString());
    }

    private GenericRecordFlatteningFn() {}
  }

  static final class TableRowFlatteningFn extends FlatRecordConvertFn<SchemaAndRecord> {

    private static TableRowFlatteningFn tableRowFlatteningFn = new TableRowFlatteningFn();

    @Override
    public KV<FlatRecord, String> apply(SchemaAndRecord input) {
      return KV.of(
          RecordFlattener.forGenericRecord().flatten(input.getRecord()),
          input.getRecord().getSchema().toString());
    }

    private TableRowFlatteningFn() {}
  }

  /**
   * Converts {@link org.apache.beam.sdk.values.Row} object to AVRO {@link
   * org.apache.avro.generic.GenericRecord}.
   */
  static final class BeamRowFlatteningFn extends FlatRecordConvertFn<Row> {

    private static BeamRowFlatteningFn beamRowFlatteningFn = new BeamRowFlatteningFn();

    @Override
    public KV<FlatRecord, String> apply(Row input) {
      var genericRecord = AvroUtils.toGenericRecord(input, null);
      return KV.of(
          RecordFlattener.forGenericRecord().flatten(genericRecord),
          genericRecord.getSchema().toString());
    }
  }

  public static GenericRecordFlatteningFn forGenericRecord() {
    return GenericRecordFlatteningFn.genericRecordFlatteningFn;
  }

  public static TableRowFlatteningFn forBigQueryTableRow() {
    return TableRowFlatteningFn.tableRowFlatteningFn;
  }

  public static BeamRowFlatteningFn forBeamRow() {
    return BeamRowFlatteningFn.beamRowFlatteningFn;
  }
}
