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

package com.google.cloud.solutions.autotokenize.pipeline.dlp;

import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Splits the columnName-values lists into chunks that are within the DLP Table limits.
 */
public class BatchColumnsForDlp extends
  PTransform<PCollection<KV<String, Iterable<Value>>>, PCollection<KV<Table, Map<String, String>>>> {

  @Override
  public PCollection<KV<Table, Map<String, String>>> expand(
    PCollection<KV<String, Iterable<Value>>> input) {

    return input
      .apply(MapElements.via(new ColumnNameKeys()))
      .apply("BatchColumnsForDlp", GroupByBatchSize.with(new DlpColumnAccumulatorFactory()))
      .setCoder(KvCoder.of(ProtoCoder.of(Table.class),
        MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));
  }

  private static class ColumnNameKeys extends SimpleFunction<KV<String, Iterable<Value>>, KV<String, KV<String, Iterable<Value>>>> {

    @Override
    public KV<String, KV<String, Iterable<Value>>> apply(KV<String, Iterable<Value>> input) {
      return KV.of(input.getKey(), input);
    }
  }


  private static class DlpColumnAccumulatorFactory
    implements BatchAccumulatorFactory<KV<String, Iterable<Value>>, KV<Table, Map<String, String>>>, Serializable {
    @Override
    public BatchAccumulator<KV<String, Iterable<Value>>, KV<Table, Map<String, String>>> newAccumulator() {
      return DlpColumnValueAccumulator.create();
    }
  }
}
