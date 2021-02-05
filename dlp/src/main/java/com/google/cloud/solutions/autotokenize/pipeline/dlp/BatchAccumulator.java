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


import com.google.common.collect.ImmutableList;
import java.util.Iterator;

/**
 * Provides mechanism to batch input data of type {@code <I>} using some business logic.
 *
 * @param <InputT> the type of input data type
 * @param <OutputT> the type of output data type
 */
public interface BatchAccumulator<InputT, OutputT> {

  /**
   * Offer one element to be added to the batch.
   *
   * @param element the element to be added.
   * @return true if addition successful, false otherwise.
   */
  boolean addElement(InputT element);

  default ImmutableList<InputT> addAllElements(Iterable<InputT> elements) {
    return addAllElements(elements.iterator());
  }

  /**
   * Adds all elements in the Iterable to the accumulator.
   *
   * @param elements the elements to add to accumulator.
   * @return List of elements that could not be added due to accumulator full.
   */
  default ImmutableList<InputT> addAllElements(Iterator<InputT> elements) {

    while (elements.hasNext()) {
      InputT element = elements.next();

      if (!addElement(element)) {
        return ImmutableList.<InputT>builder().add(element).addAll(elements).build();
      }
    }

    return ImmutableList.of();
  }

  /** Returns the accumulated elements as batch of type O. */
  Batch<OutputT> makeBatch();

  /**
   * Provides interface to access attributes of the Batch and the batched data object.
   *
   * @param <T> the type of batched data object.
   */
  interface Batch<T> {

    /** Returns the batched elements. */
    T get();

    /** The number of elements in the batch. */
    int elementsCount();

    /** The serialized size of the batch. */
    int serializedSize();

    /** A printable report of statistics. */
    String report();
  }
}
