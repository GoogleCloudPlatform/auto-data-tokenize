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

package com.google.cloud.solutions.autotokenize.pipeline;


import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.encryptors.EncryptingFlatRecordTokenizer;
import com.google.cloud.solutions.autotokenize.encryptors.ValueTokenizerFactory;
import java.util.Collection;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A composite transform that (1) Flattens a Generic Record. (2) Encrypts the Value based on
 * tokenize columns. (3) Re-nests the flat record to a GenericRecord with updated schema.
 */
@AutoValue
public abstract class ValueEncryptionTransform
    extends PTransform<PCollection<FlatRecord>, PCollection<FlatRecord>> {

  public static Builder builder() {
    return new AutoValue_ValueEncryptionTransform.Builder();
  }

  abstract ValueTokenizerFactory valueTokenizerFactory();

  abstract Collection<String> encryptColumnNames();

  @Override
  public PCollection<FlatRecord> expand(PCollection<FlatRecord> input) {
    return input.apply(
        "TinkEncryptRecordColumns",
        MapElements.into(TypeDescriptor.of(FlatRecord.class))
            .via(
                EncryptingFlatRecordTokenizer.withTokenizeSchemaKeys(encryptColumnNames())
                    .withTokenizerFactory(valueTokenizerFactory())
                    .encryptFn()));
  }

  /** Convenience builder class for the EncryptionTransform. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder valueTokenizerFactory(ValueTokenizerFactory valueTokenizerFactory);

    public abstract Builder encryptColumnNames(Collection<String> encryptColumnNames);

    public abstract ValueEncryptionTransform build();
  }
}
