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

import com.google.cloud.solutions.autotokenize.common.BatchAccumulator;
import com.google.common.flogger.GoogleLogger;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * Generic batching function that implements all the required functionality for batching.
 */
public abstract class BatchingFn<I, O> extends DoFn<I, O> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  protected static final String RECORD_BUFFER_ID = "recordBuffer";
  protected static final String EVENT_TIMER_ID = "batchingEventTimer";

  protected abstract BatchAccumulator<I, O> newAccumulator();

  public void processNewElement(I newElement, BagState<I> elementsBag, Timer eventTimer,
      BoundedWindow boundedWindow) {

    elementsBag.add(newElement);
    eventTimer.set(boundedWindow.maxTimestamp());
  }


  public void processBatch(BagState<I> bufferedElements, OutputReceiver<O> outputReceiver) {
    BatchAccumulator<I, O> accumulator = newAccumulator();

    for (I element : bufferedElements.read()) {
      if (!accumulator.addElement(element)) {
        writeOutput(accumulator.makeBatch(), outputReceiver);
        accumulator = newAccumulator();
      }

      accumulator.addElement(element);
    }

    writeOutput(accumulator.makeBatch(), outputReceiver);
  }

  private static <O> void writeOutput(BatchAccumulator.Batch<O> batch,
      OutputReceiver<O> outputReceiver) {
    outputReceiver.output(batch.get());
    logger.atInfo()
        .log("Sending Batch%nsize:%s%nElements:%s", batch.serializedSize(), batch.report());
  }


}
