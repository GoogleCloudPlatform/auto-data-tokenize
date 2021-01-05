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

package com.google.cloud.solutions.autotokenize.pipeline;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.common.RecordUnflattener;
import com.google.cloud.solutions.autotokenize.pipeline.encryptors.DaeadEncryptingValueTokenizerFactory;
import com.google.cloud.solutions.autotokenize.pipeline.encryptors.EncryptingFlatRecordTokenizer;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A composite transform that (1) Flattens a Generic Record. (2) Encrypts the Value based on
 * tokenize columns. (3) Re-nests the flat record to a GenericRecord with updated schema.
 */
@AutoValue
public abstract class ValueEncryptionTransform
    extends PTransform<PCollection<FlatRecord>, PCollection<GenericRecord>> {

  public static Builder builder() {
    return new AutoValue_ValueEncryptionTransform.Builder();
  }

  abstract DaeadEncryptingValueTokenizerFactory valueTokenizerFactory();

  abstract ImmutableSet<String> encryptColumnNames();

  abstract String encryptedSchemaJson();

  @Override
  public PCollection<GenericRecord> expand(PCollection<FlatRecord> input) {
    return input
        .apply(
            "EncryptRecordColumns",
            MapElements
                .into(TypeDescriptor.of(FlatRecord.class))
                .via(EncryptingFlatRecordTokenizer
                    .withTokenizeSchemaKeys(encryptColumnNames())
                    .withTokenizerFactory(valueTokenizerFactory())
                    .encryptFn()))
        .apply("NestAsGenericRecord", MapElements.via(new RecordNester(encryptedSchemaJson())))
        .setCoder(AvroCoder.of(new Schema.Parser().parse(encryptedSchemaJson())));
  }

  /**
   * Convert a FlatRecord to Generic Record.
   */
  private static class RecordNester extends SimpleFunction<FlatRecord, GenericRecord> {

    private final String outputSchemaString;

    public RecordNester(String outputSchemaString) {
      this.outputSchemaString = outputSchemaString;
    }

    @Override
    public GenericRecord apply(FlatRecord flatRecord) {
      Schema outputSchema = new Schema.Parser().parse(outputSchemaString);
      return RecordUnflattener.forSchema(outputSchema).unflatten(flatRecord);
    }
  }

  /**
   * Convenience builder class for the EncryptionTransform.
   */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder valueTokenizerFactory(
        DaeadEncryptingValueTokenizerFactory valueTokenizerFactory);

    public abstract Builder encryptColumnNames(ImmutableSet<String> encryptColumnNames);

    public abstract Builder encryptedSchemaJson(String encryptedSchemaJson);

    public Builder encryptedSchema(Schema encryptedSchema) {
      return encryptedSchemaJson(encryptedSchema.toString());
    }

    public abstract ValueEncryptionTransform build();
  }
}
