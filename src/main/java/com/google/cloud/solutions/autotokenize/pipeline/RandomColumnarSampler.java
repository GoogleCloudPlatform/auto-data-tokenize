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
import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.common.collect.ImmutableList;
import com.google.privacy.dlp.v2.Value;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class RandomColumnarSampler
    extends PTransform<PCollection<FlatRecord>, PCollection<KV<String, Iterable<Value>>>> {

  abstract int sampleSize();

  public static RandomColumnarSampler any(int sampleSize) {
    checkArgument(
        sampleSize >= 0,
        "provide a positive number for sampleSize or 0 for no-sampling. Found %s",
        sampleSize);
    return new AutoValue_RandomColumnarSampler(sampleSize);
  }

  @Override
  public PCollection<KV<String, Iterable<Value>>> expand(PCollection<FlatRecord> input) {

    return input
        .apply("SplitIntoSchemaColumns", MapElements.via(new SplitRecordByKeys()))
        .apply(Flatten.iterables())
        .apply(Filter.by(kv -> !kv.getValue().equals(Value.getDefaultInstance())))
        .apply(
            "MakeBatches",
            (sampleSize() == 0)
                ? GroupIntoBatches.ofSize(10000)
                : Sample.fixedSizePerKey(sampleSize()));
  }

  private static class SplitRecordByKeys
      extends SimpleFunction<FlatRecord, List<KV<String, Value>>> {

    @Override
    public ImmutableList<KV<String, Value>> apply(FlatRecord input) {

      Map<String, String> flatKeySchemaKeyMap = input.getFlatKeySchemaMap();

      return input.getValuesMap().entrySet().stream()
          .map(
              valueEntry ->
                  KV.of(flatKeySchemaKeyMap.get(valueEntry.getKey()), valueEntry.getValue()))
          .collect(toImmutableList());
    }
  }
}
