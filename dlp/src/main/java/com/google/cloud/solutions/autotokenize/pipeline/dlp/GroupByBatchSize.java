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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.flogger.GoogleLogger;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Generic batching function that implements all the required functionality for batching.
 */
@AutoValue
public abstract class GroupByBatchSize<K, InputT, OutputT> extends PTransform<PCollection<KV<K, InputT>>, PCollection<OutputT>> {

  abstract BatchAccumulatorFactory<InputT, OutputT> batchAccumulatorFactory();

  @Nullable
  abstract Duration maxBufferDuration();

  public static <K, InputT, OutputT> GroupByBatchSize<K, InputT, OutputT> with(BatchAccumulatorFactory<InputT, OutputT> accumulatorFactory) {
    return new AutoValue_GroupByBatchSize.Builder<K,InputT, OutputT>().batchAccumulatorFactory(accumulatorFactory).build();
  }

//  public static <K, InputT, OutputT> GroupByBatchSize<K, InputT, OutputT> with(Class<K> keyType, BatchAccumulatorFactory<InputT, OutputT> accumulatorFactory) {
//    return new AutoValue_GroupByBatchSize.Builder<K, InputT, OutputT>().keyType(keyType).batchAccumulatorFactory(accumulatorFactory).build();
//  }

  public GroupByBatchSize<K, InputT, OutputT> withMaxBufferDuration(Duration duration) {
    checkArgument(duration != null && duration.isLongerThan(Duration.ZERO), "Provide non-zero buffer duration");
    return toBuilder().maxBufferDuration(duration).build();
  }

  @AutoValue.Builder
  public abstract static class Builder<K, InputT, OutputT> {
    abstract Builder<K, InputT, OutputT> batchAccumulatorFactory(BatchAccumulatorFactory<InputT, OutputT> batchAccumulatorFactory);

//    abstract Builder<K, InputT, OutputT> keyType(Class<K> keyType);

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
    Coder<K> keyCoder = (Coder<K>) inputCoder.getCoderArguments().get(0);
    Coder<InputT> valueCoder = (Coder<InputT>) inputCoder.getCoderArguments().get(1);

    return input.apply(
      ParDo.of(
        new BatchBySizeFn<>(
          allowedLateness,
          maxBufferDuration(),
          keyCoder,
          valueCoder,
          batchAccumulatorFactory())));
  }

  static class BatchBySizeFn<K, InputT, OutputT> extends DoFn<KV<K, InputT>, OutputT> {
    private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

    private static final String END_OF_WINDOW_ID = "endOFWindow";
    private static final String END_OF_BUFFERING_ID = "endOfBuffering";
    private static final String BATCH_ID = "batch";
    private static final String NUM_ELEMENTS_IN_BATCH_ID = "numElementsInBatch";

    private final Duration allowedLateness;
    private final Duration maxBufferingDuration;

    @TimerId(END_OF_WINDOW_ID)
    private final TimerSpec windowTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId(END_OF_BUFFERING_ID)
    private final TimerSpec bufferingTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @StateId(BATCH_ID)
    private final StateSpec<BagState<InputT>> batchSpec;

    @StateId(NUM_ELEMENTS_IN_BATCH_ID)
    private final StateSpec<CombiningState<Long, long[], Long>> numElementsInBatchSpec;

    private final BatchAccumulatorFactory<InputT, OutputT> accumulatorFactory;

    BatchBySizeFn(
      Duration allowedLateness,
      Duration maxBufferingDuration,
      Coder<K> inputKeyCoder,
      Coder<InputT> inputValueCoder,
      BatchAccumulatorFactory<InputT, OutputT> accumulatorFactory) {
      this.allowedLateness = allowedLateness;
      this.maxBufferingDuration = maxBufferingDuration;
      this.batchSpec = StateSpecs.bag(inputValueCoder);
      this.numElementsInBatchSpec =
        StateSpecs.combining(
          new Combine.BinaryCombineLongFn() {

            @Override
            public long identity() {
              return 0L;
            }

            @Override
            public long apply(long left, long right) {
              return left + right;
            }
          });

      this.accumulatorFactory = accumulatorFactory;
    }

    @ProcessElement
    public void processElement(
      @TimerId(END_OF_WINDOW_ID) Timer windowTimer,
      @TimerId(END_OF_BUFFERING_ID) Timer bufferingTimer,
      @StateId(BATCH_ID) BagState<InputT> buffer,
      @StateId(NUM_ELEMENTS_IN_BATCH_ID) CombiningState<Long, long[], Long> numElementsInBatch,
      @Element KV<K, InputT> element,
      BoundedWindow window,
      OutputReceiver<OutputT> receiver) {
      Instant windowEnds = window.maxTimestamp().plus(allowedLateness);
      logger.atFine().log("*** SET TIMER *** to point in time %s for window %s", windowEnds, window);
      windowTimer.set(windowEnds);


      logger.atFine().log("*** BATCH *** Add element for window %s", window);
      buffer.add(element.getValue());
      // Blind add is supported with combiningState
      numElementsInBatch.add(1L);

      long num = numElementsInBatch.read();
      if (num == 1 && maxBufferingDuration != null) {
        // This is the first element in batch. Start counting buffering time if a limit was set.
        bufferingTimer.offset(maxBufferingDuration).setRelative();
      }

      BatchedElements<InputT, OutputT> batchedElements =
        new BatchMaker<>(/*noUnbatched*/false, accumulatorFactory).makeBatches(buffer);

      if (!batchedElements.batches().isEmpty()) {
        flushBatch(batchedElements, receiver, buffer, numElementsInBatch, bufferingTimer);
      }
    }

    @OnTimer(END_OF_BUFFERING_ID)
    public void onBufferingTimer(
      OutputReceiver<OutputT> receiver,
      @Timestamp Instant timestamp,
      @StateId(BATCH_ID) BagState<InputT> buffer,
      @StateId(NUM_ELEMENTS_IN_BATCH_ID) CombiningState<Long, long[], Long> numElementsInBatch,
      @TimerId(END_OF_BUFFERING_ID) Timer bufferingTimer) {
      logger.atFine().log(
        "*** END OF BUFFERING *** for timer timestamp %s with buffering duration %s",
        timestamp,
        maxBufferingDuration);

      BatchedElements<InputT, OutputT> batchedElements =
        new BatchMaker<>(/*noUnbatched=*/true, accumulatorFactory).makeBatches(buffer);


      flushBatch(batchedElements, receiver, buffer, numElementsInBatch, null);
    }

    @OnTimer(END_OF_WINDOW_ID)
    public void onWindowTimer(
      OutputReceiver<OutputT> receiver,
      @Timestamp Instant timestamp,
      @StateId(BATCH_ID) BagState<InputT> buffer,
      @StateId(NUM_ELEMENTS_IN_BATCH_ID) CombiningState<Long, long[], Long> numElementsInBatch,
      @TimerId(END_OF_BUFFERING_ID) Timer bufferingTimer,
      BoundedWindow window) {
      logger.atFine().log(
        "*** END OF WINDOW *** for timer timestamp %s in windows %s",
        timestamp,
        window.toString());

      BatchedElements<InputT, OutputT> batchedElements =
        new BatchMaker<>(/*noUnbatched=*/ true, accumulatorFactory).makeBatches(buffer);

      flushBatch(batchedElements, receiver, buffer, numElementsInBatch, bufferingTimer);
    }

    private static class BatchMaker<InputT, OutputT> {
      private final boolean noUnbatched;
      private final BatchAccumulatorFactory<InputT, OutputT> accumulatorFactory;

      public BatchMaker(boolean noUnbatched, BatchAccumulatorFactory<InputT, OutputT> accumulatorFactory) {
        this.noUnbatched = noUnbatched;
        this.accumulatorFactory = accumulatorFactory;
      }

      public BatchedElements<InputT, OutputT> makeBatches(BagState<InputT> buffer) {
        ImmutableList.Builder<BatchAccumulator.Batch<OutputT>> batchBuilder = ImmutableList.builder();

        BatchAccumulator<InputT, OutputT> accumulator = accumulatorFactory.newAccumulator();
        Iterable<InputT> unBatchedElements = buffer.read();

        while (!Iterables.isEmpty(unBatchedElements = accumulator.addAllElements(unBatchedElements))) {
          batchBuilder.add(accumulator.makeBatch());
          accumulator = accumulatorFactory.newAccumulator();
        }

        if (noUnbatched) {
          batchBuilder.add(accumulator.makeBatch());
        }

        return BatchedElements.<InputT, OutputT>builder()
          .batches(batchBuilder.build())
          .unBatchedElements(ImmutableList.copyOf(unBatchedElements))
          .build();
      }

    }

    @AutoValue
    static abstract class BatchedElements<InputT, OutputT> {

      abstract ImmutableList<BatchAccumulator.Batch<OutputT>> batches();

      abstract ImmutableList<InputT> unBatchedElements();

      public static <InputT, OutputT> Builder<InputT, OutputT> builder() {
        return new AutoValue_GroupByBatchSize_BatchBySizeFn_BatchedElements.Builder<>();
      }

      @AutoValue.Builder
      static abstract class Builder<InputT, OutputT> {
        public abstract Builder<InputT, OutputT> batches(ImmutableList<BatchAccumulator.Batch<OutputT>> batches);

        public abstract Builder<InputT, OutputT> unBatchedElements(ImmutableList<InputT> unBatchedElements);

        public abstract BatchedElements<InputT, OutputT> build();
      }
    }

    //outputs the batch
    private void flushBatch(
      BatchedElements<InputT, OutputT> batchedElements,
      OutputReceiver<OutputT> receiver,
      BagState<InputT> buffer,
      CombiningState<Long, long[], Long> numElementsInBatch,
      @Nullable Timer bufferingTimer) {

      batchedElements.batches().forEach(batch -> sendBatchAndLog(batch, receiver));

      buffer.clear();
      logger.atFine().log("*** BATCH *** clear");
      batchedElements.unBatchedElements().forEach(buffer::add);
      logger.atFine().log("*** ADDED unflushed elements: %s", batchedElements.unBatchedElements().size());
      numElementsInBatch.clear();
      numElementsInBatch.add((long) batchedElements.unBatchedElements().size());
      // We might reach here due to batch size being reached or window expiration. Reset the
      // buffering timer (if not null) since the state is empty now. It'll be extended again if a
      // new element arrives prior to the expiration time set here.
      // TODO(BEAM-10887): Use clear() when it's available.
      if (bufferingTimer != null && maxBufferingDuration != null) {
        bufferingTimer.offset(maxBufferingDuration).setRelative();
      }
    }

    private static <OutputT> void sendBatchAndLog(BatchAccumulator.Batch<OutputT> batch, OutputReceiver<OutputT> receiver) {
      receiver.output(batch.get());
      logger.atFine().log("**** Batched Report:%n%s", batch.report());
    }
  }
}
