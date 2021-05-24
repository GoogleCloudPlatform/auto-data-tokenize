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

package com.google.cloud.solutions.autotokenize.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.tuple.ImmutablePair;

public final class PairIterator<L, R> implements Iterator<ImmutablePair<L, R>> {

  private final Iterator<L> leftIterator;
  private final Iterator<R> rightIterator;

  public PairIterator(Iterable<L> leftIterable, Iterable<R> rightIterable) {
    this.leftIterator = checkNotNull(leftIterable).iterator();
    this.rightIterator = checkNotNull(rightIterable).iterator();
  }

  public static <L, R> PairIterator<L, R> of(Iterable<L> left, Iterable<R> right) {
    return new PairIterator<>(left, right);
  }

  public Stream<ImmutablePair<L, R>> stream() {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(this, Spliterator.ORDERED), false);
  }

  @Override
  public boolean hasNext() {
    return leftIterator.hasNext() && rightIterator.hasNext();
  }

  @Override
  public ImmutablePair<L, R> next() {
    if (!hasNext()) {
      throw new NoSuchElementException("no element found");
    }

    return ImmutablePair.of(leftIterator.next(), rightIterator.next());
  }
}
