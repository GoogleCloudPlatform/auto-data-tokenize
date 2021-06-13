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

import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.common.collect.ImmutableSet;
import com.google.privacy.dlp.v2.FieldId;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

/** Checks if the column header matches one of the tokenization columns with arrays. */
public class TokenizingColPatternChecker implements Serializable {

  private final ImmutableSet<JsonPathArrayPatternMatcher> tokenizingPatternMatchers;

  private TokenizingColPatternChecker(Collection<String> fieldNames) {
    this.tokenizingPatternMatchers =
        fieldNames.stream().map(JsonPathArrayPatternMatcher::of).collect(toImmutableSet());
  }

  public static TokenizingColPatternChecker of(Collection<String> fieldNames) {
    return new TokenizingColPatternChecker(fieldNames);
  }

  public static TokenizingColPatternChecker of(String... fieldNames) {
    return new TokenizingColPatternChecker(Arrays.asList(fieldNames));
  }

  public boolean isTokenizeColumn(String columnName) {
    return tokenizingPatternMatchers.stream()
        .map(matcher -> matcher.matches(columnName))
        .reduce(Boolean::logicalOr)
        .orElse(Boolean.FALSE);
  }

  public boolean isTokenizeColumn(FieldId fieldId) {
    return (fieldId != null) && isTokenizeColumn(fieldId.getName());
  }

  @Override
  public String toString() {
    return "TokenizingColPatternChecker{"
        + "tokenizingPatternMatchers="
        + tokenizingPatternMatchers
        + '}';
  }
}
