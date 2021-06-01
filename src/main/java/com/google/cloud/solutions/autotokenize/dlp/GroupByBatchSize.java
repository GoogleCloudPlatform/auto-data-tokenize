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

package com.google.cloud.solutions.autotokenize.dlp;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.dlp.BatchAccumulator.Batch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.flogger.GoogleLogger;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Generic batching function that implements all the required functionality for batching. */
@AutoValue
public abstract class GroupByBatchSize<K, InputT, OutputT>
    extends PTransform<PCollection<KV<K, InputT>>, PCollection<OutputT>> {

  abstract BatchAccumulatorFactory<InputT, OutputT> batchAccumulatorFactory();

  abstract Duration maxBufferDuration();

  public static <K, InputT, OutputT> GroupByBatchSize<K, InputT, OutputT> with(
      BatchAccumulatorFactory<InputT, OutputT> accumulatorFactory) {
    return new AutoValue_GroupByBatchSize.Builder<K, InputT, OutputT>()
        .batchAccumulatorFactory(accumulatorFactory)
        .maxBufferDuration(Duration.standardMinutes(2))
        .build();
  }

  public GroupByBatchSize<K, InputT, OutputT> withMaxBufferDuration(Duration duration) {
    checkArgument(
        duration != null && duration.isLongerThan(Duration.ZERO),
        "Provide non-zero buffer duration");
    return toBuilder().maxBufferDuration(duration).build();
  }

  @AutoValue.Builder
  public abstract static class Builder<K, InputT, OutputT> {

    abstract Builder<K, InputT, OutputT> batchAccumulatorFactory(
        BatchAccumulatorFactory<InputT, OutputT> batchAccumulatorFactory);

    abstract Builder<K, InputT, OutputT> maxBufferDuration(Duration maxBufferDuration);

    abstract GroupByBatchSize<K, InputT, OutputT> build();
  }

  abstract Builder<K, InputT, OutputT> toBuilder();

  @Override
  public PCollection<OutputT> expand(PCollection<KV<K, InputT>> input) {
    Duration allowedLateness = input.getWindowingStrategy().getAllowedLateness();

    checkArgument(
        input.getCoder() instanceof KvCoder,
        "coder specified in the input PCollection is not a KvCoder");
    KvCoder<K, InputT> inputCoder = (KvCoder<K, InputT>) input.getCoder();

    return input.apply(
        ParDo.of(
            new BatchBySizeFn<>(
                allowedLateness,
                maxBufferDuration(),
                inputCoder.getKeyCoder(),
                inputCoder.getValueCoder(),
                batchAccumulatorFactory())));
  }

  static class BatchBySizeFn<K, InputT, OutputT> extends DoFn<KV<K, InputT>, OutputT> {

    private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

    private static final String END_OF_WINDOW_ID = "endOFWindow";
    private static final String END_OF_BUFFERING_ID = "endOfBuffering";
    private static final String BATCH_ID = "batch";

    private final Duration allowedLateness;
    private final Duration maxBufferingDuration;

    @TimerId(END_OF_WINDOW_ID)
    private final TimerSpec windowTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId(END_OF_BUFFERING_ID)
    private final TimerSpec bufferingTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @StateId(BATCH_ID)
    private final StateSpec<ValueState<Iterable<InputT>>> batchedElementsSpec;

    private final BatchAccumulatorFactory<InputT, OutputT> accumulatorFactory;

    BatchBySizeFn(
        Duration allowedLateness,
        Duration maxBufferingDuration,
        Coder<K> inputKeyCoder,
        Coder<InputT> inputValueCoder,
        BatchAccumulatorFactory<InputT, OutputT> accumulatorFactory) {
      this.allowedLateness = allowedLateness;
      this.maxBufferingDuration = maxBufferingDuration;
      this.batchedElementsSpec = StateSpecs.value(IterableCoder.of(inputValueCoder));
      this.accumulatorFactory = accumulatorFactory;
    }

    @ProcessElement
    public void processElement(
        @TimerId(END_OF_WINDOW_ID) Timer windowTimer,
        @TimerId(END_OF_BUFFERING_ID) Timer bufferingTimer,
        @StateId(BATCH_ID) ValueState<Iterable<InputT>> buffer,
        @Element KV<K, InputT> element,
        BoundedWindow window,
        OutputReceiver<OutputT> receiver) {
      Instant windowEnds = window.maxTimestamp().plus(allowedLateness);
      logger.atFine().log(
          "*** SET TIMER *** to point in time %s for window %s", windowEnds, window);
      windowTimer.set(windowEnds);

      logger.atFine().log("*** BATCH *** Add element for window %s", window);

      var batchState = batchState(buffer);
      var isEmpty = batchState.isStateEmpty();

      batchState.addElement(element.getValue());
      var batches = batchState.makeBatches();

      if (!batches.batches().isEmpty()) {
        flushBatch(batchState, receiver, bufferingTimer);
      } else if (isEmpty && maxBufferingDuration != null) {
        // This is the first element in batch. Start counting buffering time if a limit was set.
        bufferingTimer.offset(maxBufferingDuration).setRelative();
      }
    }

    private BatchState batchState(ValueState<Iterable<InputT>> buffer) {
      return new BatchState(buffer, false);
    }

    private BatchState batchStateWithForcePartial(ValueState<Iterable<InputT>> buffer) {
      return new BatchState(buffer, true);
    }

    @OnTimer(END_OF_BUFFERING_ID)
    public void onBufferingTimer(
        OutputReceiver<OutputT> receiver,
        @Timestamp Instant timestamp,
        @StateId(BATCH_ID) ValueState<Iterable<InputT>> buffer,
        @TimerId(END_OF_BUFFERING_ID) Timer bufferingTimer) {
      logger.atFine().log(
          "*** END OF BUFFERING *** for timer timestamp %s with buffering duration %s",
          timestamp, maxBufferingDuration);
      flushBatch(batchStateWithForcePartial(buffer), receiver, bufferingTimer);
    }

    @OnTimer(END_OF_WINDOW_ID)
    public void onWindowTimer(
        OutputReceiver<OutputT> receiver,
        @Timestamp Instant timestamp,
        @StateId(BATCH_ID) ValueState<Iterable<InputT>> buffer,
        @TimerId(END_OF_BUFFERING_ID) Timer bufferingTimer,
        BoundedWindow window) {
      logger.atFine().log(
          "*** END OF WINDOW *** for timer timestamp %s in windows %s",
          timestamp, window.toString());
      flushBatch(batchStateWithForcePartial(buffer), receiver, bufferingTimer);
    }

    /** Write the accumulated values to ParDo output. */
    private void flushBatch(
        BatchState batchState, OutputReceiver<OutputT> receiver, @Nullable Timer bufferingTimer) {

      var batchedElements = batchState.makeBatches();

      if (batchedElements.batches().isEmpty()) {
        return;
      }

      batchedElements.batches().forEach(batch -> sendBatchAndLog(batch, receiver));
      batchedElements.replaceStateWithUnbufferedElements();
      // We might reach here due to batch size being reached or window expiration. Reset the
      // buffering timer (if not null) since the state is empty now. It'll be extended again
      // if a
      // new element arrives prior to the expiration time set here.
      // TODO(BEAM-10887): Use clear() when it's available.
      if (bufferingTimer != null && maxBufferingDuration != null) {
        bufferingTimer.offset(maxBufferingDuration).setRelative();
      }
    }

    private static <OutputT> void sendBatchAndLog(
        BatchAccumulator.Batch<OutputT> batch, OutputReceiver<OutputT> receiver) {
      receiver.output(batch.get());
      logger.atFine().log("**** Batched Report:%n%s", batch.report());
    }

    /** Helper class to easily manipulate the accumulated elements. */
    private final class BatchState {

      private final ValueState<Iterable<InputT>> batchedState;
      private final boolean allowPartialBatch;

      private BatchState(ValueState<Iterable<InputT>> batchedState, boolean allowPartialBatch) {
        this.batchedState = batchedState;
        this.allowPartialBatch = allowPartialBatch;
      }

      public boolean isStateEmpty() {
        return read().isEmpty();
      }

      public synchronized ImmutableList<InputT> addElement(InputT newElement) {
        return addElements(ImmutableList.of(newElement));
      }

      /** Returns the list of accumulated elements after adding the new value. */
      private synchronized ImmutableList<InputT> addElements(Iterable<InputT> newElements) {
        var updatedValues =
            ImmutableList.<InputT>builder().addAll(read()).addAll(newElements).build();
        batchedState.write(updatedValues);
        return updatedValues;
      }

      private synchronized ImmutableList<InputT> read() {
        var values = batchedState.read();
        return (values == null) ? ImmutableList.of() : ImmutableList.copyOf(values);
      }

      private BatchedElements makeBatches() {
        var batchBuilder = ImmutableList.<BatchAccumulator.Batch<OutputT>>builder();

        var accumulator = accumulatorFactory.newAccumulator();
        var unBatchedElements = read();

        while (!Iterables.isEmpty(
            unBatchedElements = accumulator.addAllElements(unBatchedElements))) {
          batchBuilder.add(accumulator.makeBatch());
          accumulator = accumulatorFactory.newAccumulator();
        }

        if (allowPartialBatch) {
          batchBuilder.add(accumulator.makeBatch());
        }

        return new BatchedElements(batchBuilder.build(), ImmutableList.copyOf(unBatchedElements));
      }

      public synchronized void replaceStateWithElements(ImmutableList<InputT> elements) {
        batchedState.clear();
        batchedState.write(elements);
      }

      private final class BatchedElements {

        private final ImmutableList<BatchAccumulator.Batch<OutputT>> batches;

        private final ImmutableList<InputT> unBatchedElements;

        public ImmutableList<Batch<OutputT>> batches() {
          return batches;
        }

        public ImmutableList<InputT> unBatchedElements() {
          return unBatchedElements;
        }

        public BatchedElements(
            ImmutableList<Batch<OutputT>> batches, ImmutableList<InputT> unBatchedElements) {
          this.batches = batches;
          this.unBatchedElements = unBatchedElements;
        }

        public void replaceStateWithUnbufferedElements() {
          replaceStateWithElements(unBatchedElements);
        }
      }
    }
  }
}
