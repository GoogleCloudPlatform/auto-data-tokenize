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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Returns an {@link Iterator} in which each element is the result of passing the corresponding
 * element of each of the input {@code List}.
 *
 * <p>For example:
 *
 * <pre>{@code
 * ZippingIterator<String> zipIterator =
 *   ZippingIterator.create(
 *       ImmutableList.of("a","b","c", "d"),
 *       ImmutableList.of("1","2","3"),
 *       ImmutableList.of("I","II","III"));
 *
 * List<Optional<String>> element = zipIterator.next(); // element = ["a","1", "I"];
 * }</pre>
 *
 * <p>The resulting iterator  will only be as long as the longest of the input lists;
 * It will return {@code Optinal.empty()} for list-elements when index is not present in
 * corresponding list.
 *
 * e.g.
 * <pre>{@code
 * ZippingIterator<String> zipIterator =
 *   ZippingIterator.create(
 *     Lists.of(
 *       ImmutableList.of("a","b","c", "d"),
 *       ImmutableList.of("1","2","3"),
 *       ImmutableList.of("I","II","III")));
 *
 * zipIterator.next(); // element = ["a","1", "I"];
 * zipIterator.next(); // element = ["b","2", "II"];
 * zipIterator.next(); // element = ["c","3", "III"];
 * List<Optional<String>> element = zipIterator.next(); // element = [Optional.of("d"),empty(), empty()];
 * }</pre>
 *
 * <p><b>Performance note:</b> The resulting Iterator is not efficiently splittable.
 * This may harm parallel performance.
 *
 * @param <T> the type of input collections to zip
 */
public class ZippingIterator<T> implements Iterator<ImmutableList<Optional<T>>> {

  private final int maxSize;
  private final ImmutableList<ImmutableList<T>> inputLists;

  int iterIndex = 0;

  /**
   * Creates a zipping iterator for the given list of lists.
   */
  public static <T> ZippingIterator<T> create(List<List<T>> inputLists) {
    return new ZippingIterator<>(inputLists);
  }

  /**
   * Provides a stream of elements generated from the ZippingIterator
   *
   * <pre>{@code
   * ZippingIterator
   *   .createStream(
   *     Lists.of(
   *       ImmutableList.of("a","b","c", "d"),
   *       ImmutableList.of("1","2","3"),
   *       ImmutableList.of("I","II","III")))
   *   .map((List<Optional<String>> elements) -> ...)
   * }</pre>
   */
  public static <T> Stream<List<Optional<T>>> createElementStream(List<List<T>> inputLists) {
    return StreamSupport
        .stream(Spliterators.spliteratorUnknownSize(create(inputLists), Spliterator.ORDERED),
            false);
  }

  public ZippingIterator(List<List<T>> inputLists) {
    checkNotNull(inputLists, "provide input lists to zip");

    ImmutableList.Builder<ImmutableList<T>> listBuilder = ImmutableList.builder();

    int maxSize = 0;

    for (List<T> l : inputLists) {
      int size = l.size();
      listBuilder.add(ImmutableList.copyOf(l));
      if (size > maxSize) {
        maxSize = size;
      }
    }

    this.inputLists = listBuilder.build();
    this.maxSize = inputLists.stream().mapToInt(List::size).max().orElse(0);

  }

  @Override
  public boolean hasNext() {
    return (iterIndex < maxSize);
  }

  @Override
  public ImmutableList<Optional<T>> next() {
    if (!hasNext()) {
      throw new NoSuchElementException("no element at " + iterIndex + ", size: " + maxSize);
    }

    ImmutableList<Optional<T>> elements =
        inputLists.stream()
            .map(l -> (iterIndex < l.size()) ? Optional.of(l.get(iterIndex)) : Optional.<T>empty())
            .collect(toImmutableList());

    // increment the iterator to point to the next element;
    iterIndex++;

    return elements;
  }
}
