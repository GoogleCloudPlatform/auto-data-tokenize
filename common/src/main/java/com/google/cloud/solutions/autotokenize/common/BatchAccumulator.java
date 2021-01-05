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

package com.google.cloud.solutions.autotokenize.common;

/**
 * Provides mechanism to batch input data of type {@code <I>} using some business logic.
 *
 * @param <I> the type of input data type
 * @param <O> the type of output data type
 */
public interface BatchAccumulator<I, O> {

  /**
   * Offer one element to be added to the batch.
   *
   * @param element the element to be added.
   * @return true if addition successful, false otherwise.
   */
  boolean addElement(I element);

  /**
   * Returns the accumulated elements as batch of type O.
   */
  Batch<O> makeBatch();


  /**
   * Provides interface to access attributes of the Batch and the batched data object.
   *
   * @param <O> the type of batched data object.
   */
  interface Batch<O> {

    /**
     * Returns the batched elements.
     */
    O get();

    /**
     * The number of elements in the batch.
     */
    int elementsCount();

    /**
     * The serialized size of the batch.
     */
    int serializedSize();

    /**
     * A printable report of statistics.
     */
    String report();
  }
}
