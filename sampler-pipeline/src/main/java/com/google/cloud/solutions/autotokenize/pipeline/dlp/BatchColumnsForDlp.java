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

package com.google.cloud.solutions.autotokenize.pipeline.dlp;

import com.google.cloud.solutions.autotokenize.common.DlpColumnValueAccumulator;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.util.Map;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
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

    return input.apply("BatchColumnsForDlp",
        ParDo.of(new ColumnValueBatchFn()))
        .setCoder(KvCoder.of(ProtoCoder.of(Table.class),
            MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));
  }

  /**
   * A Batching function that uses stateful and timely processing functionality to accumulate
   * elements and splits into desired batch sizes.
   */
  private static class ColumnValueBatchFn extends
      BatchingFn<KV<String, Iterable<Value>>, KV<Table, Map<String, String>>> {

    @StateId(RECORD_BUFFER_ID)
    private final StateSpec<BagState<KV<String, Iterable<Value>>>> bufferedElementsState = StateSpecs
        .bag();

    @TimerId(EVENT_TIMER_ID)
    private final TimerSpec bufferExpirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @Override
    protected DlpColumnValueAccumulator newAccumulator() {
      return DlpColumnValueAccumulator.create();
    }

    @ProcessElement
    public void processColumnValues(
        @Element KV<String, Iterable<Value>> newElement,
        @StateId(RECORD_BUFFER_ID) BagState<KV<String, Iterable<Value>>> elementsBag,
        @TimerId(EVENT_TIMER_ID) Timer eventTimer,
        BoundedWindow boundedWindow) {
      super.processNewElement(newElement, elementsBag, eventTimer, boundedWindow);
    }

    @OnTimer(EVENT_TIMER_ID)
    public void processBatch(
        @StateId(RECORD_BUFFER_ID) BagState<KV<String, Iterable<Value>>> bufferedElements,
        OutputReceiver<KV<Table, Map<String, String>>> outputReceiver) {
      super.processBatch(bufferedElements, outputReceiver);
    }
  }

}
