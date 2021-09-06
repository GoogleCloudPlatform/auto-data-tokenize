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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.common.UnFlattenKvFn;
import com.google.privacy.dlp.v2.Value;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class RandomColumnarSampler
    extends PTransform<PCollection<FlatRecord>, PCollection<KV<String, Value>>> {

  abstract int sampleSize();

  public static RandomColumnarSampler any(int sampleSize) {
    checkArgument(
        sampleSize >= 0,
        "provide a positive number for sampleSize or 0 for no-sampling. Found %s",
        sampleSize);
    return new AutoValue_RandomColumnarSampler(sampleSize);
  }

  @Override
  public PCollection<KV<String, Value>> expand(PCollection<FlatRecord> input) {

    var elements =
        input
            .apply("SplitIntoSchemaColumns", ParDo.of(new SplitRecordByKeysFn()))
            .apply(Filter.by(kv -> !Value.getDefaultInstance().equals(kv.getValue())));

    if (sampleSize() == 0) {
      return elements;
    }

    return elements
        .apply("MakeBatches", Sample.fixedSizePerKey(sampleSize()))
        .apply("UnbundleValues", ParDo.of(new UnFlattenKvFn<>()));
  }

  private static class SplitRecordByKeysFn extends DoFn<FlatRecord, KV<String, Value>> {

    @ProcessElement
    public void splitValues(
        @Element FlatRecord input, OutputReceiver<KV<String, Value>> outputReceiver) {

      Map<String, String> flatKeySchemaKeyMap = input.getFlatKeySchemaMap();

      input.getValuesMap().entrySet().stream()
          .filter(kv -> kv.getValue() != null && !kv.getValue().equals(Value.getDefaultInstance()))
          .map(
              valueEntry ->
                  KV.of(flatKeySchemaKeyMap.get(valueEntry.getKey()), valueEntry.getValue()))
          .forEach(outputReceiver::output);
    }
  }
}
