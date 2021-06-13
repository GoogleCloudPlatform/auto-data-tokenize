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

package com.google.cloud.solutions.autotokenize.testing;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.lang3.tuple.Triple;

public class ArrayPatternCheckingKeyMap<X, Y, T> implements Map<String, T>, Serializable {

  private final ImmutableSet<Triple<JsonPathArrayPatternMatcher, X, Y>> matcherValuesSet;
  private final SerializableFunction<X, String> keyTranslationFunction;
  private final SerializableFunction<Y, T> valueTranslationFunction;

  public ArrayPatternCheckingKeyMap(
      Map<X, Y> patternValueMap,
      SerializableFunction<X, String> keyTranslationFunction,
      SerializableFunction<Y, T> valueTranslationFunction) {
    this.matcherValuesSet =
        patternValueMap.entrySet().stream()
            .map(
                entry ->
                    Triple.of(
                        JsonPathArrayPatternMatcher.of(
                            keyTranslationFunction.apply(entry.getKey())),
                        entry.getKey(),
                        entry.getValue()))
            .collect(toImmutableSet());

    this.keyTranslationFunction = keyTranslationFunction;
    this.valueTranslationFunction = valueTranslationFunction;
  }

  public static <Y, T> ArrayPatternCheckingKeyMap<String, Y, T> withValueComputeFunction(
      Map<String, Y> valueMap, SerializableFunction<Y, T> valueComputeFunction) {
    return new ArrayPatternCheckingKeyMap<>(valueMap, IdentifyFunction.of(), valueComputeFunction);
  }

  @Override
  public int size() {
    return matcherValuesSet.size();
  }

  @Override
  public boolean isEmpty() {
    return matcherValuesSet.isEmpty();
  }

  @Override
  public boolean containsKey(Object o) {
    checkArgument(o instanceof String);

    return matcherValuesSet.stream()
        .map(Triple::getLeft)
        .map(matcher -> matcher.matches((String) o))
        .reduce(Boolean::logicalOr)
        .orElse(Boolean.TRUE);
  }

  @Override
  public boolean containsValue(Object o) {
    return matcherValuesSet.stream().map(Triple::getRight).anyMatch(y -> y.equals(o));
  }

  @Override
  public T get(Object o) {
    return matcherValuesSet.stream()
        .filter(entry -> entry.getLeft().matches(keyTranslationFunction.apply((X) o)))
        .map(Triple::getRight)
        .findFirst()
        .map(valueTranslationFunction::apply)
        .orElse(null);
  }

  @Override
  public T put(String s, T t) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends String, ? extends T> map) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {}

  @Override
  public Set<String> keySet() {
    return matcherValuesSet.stream()
        .map(Triple::getMiddle)
        .map(keyTranslationFunction::apply)
        .collect(toImmutableSet());
  }

  @Override
  public Collection<T> values() {
    return matcherValuesSet.stream()
        .map(Triple::getRight)
        .map(valueTranslationFunction::apply)
        .collect(toImmutableSet());
  }

  @Override
  public Set<Entry<String, T>> entrySet() {
    return matcherValuesSet.stream()
        .map(
            p ->
                Map.entry(
                    keyTranslationFunction.apply(p.getMiddle()),
                    valueTranslationFunction.apply(p.getRight())))
        .collect(toImmutableSet());
  }

  public static class IdentifyFunction<Z> implements SerializableFunction<Z, Z> {

    public static <Z> IdentifyFunction<Z> of() {
      return new IdentifyFunction<>();
    }

    @Override
    public Z apply(Z input) {
      return input;
    }
  }
}
