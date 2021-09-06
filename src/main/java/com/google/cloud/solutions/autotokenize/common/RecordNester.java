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


import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/** Convert a FlatRecord to Generic Record and set Beam schema accordingly. */
@AutoValue
public abstract class RecordNester
    extends PTransform<PCollection<FlatRecord>, PCollection<GenericRecord>> {

  abstract String schemaJson();

  public static RecordNester forSchemaJson(String schemaJson) {
    return new AutoValue_RecordNester(schemaJson);
  }

  public static RecordNester forSchema(Schema outputSchema) {
    return forSchemaJson(outputSchema.toString());
  }

  @Override
  public PCollection<GenericRecord> expand(PCollection<FlatRecord> flatRecords) {
    return flatRecords
        .apply("ReNestRecords", MapElements.via(new RecordNesterFn(schemaJson())))
        .setCoder(AvroGenericCoder.of(new Schema.Parser().parse(schemaJson())));
  }

  private static class RecordNesterFn extends SimpleFunction<FlatRecord, GenericRecord> {

    private final String outputSchemaString;

    private RecordNesterFn(String outputSchemaString) {
      this.outputSchemaString = outputSchemaString;
    }

    @Override
    public GenericRecord apply(FlatRecord flatRecord) {
      return RecordUnflattener.forSchema(new Schema.Parser().parse(outputSchemaString))
          .unflatten(flatRecord);
    }
  }
}
