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

package com.google.cloud.solutions.autotokenize.testing.stubs.dlp;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.FieldTransformation;
import java.util.Collection;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class TokenizingColPatternChecker {
  private static final Pattern REGEX_SPECIAL_CHARS =
      Pattern.compile("([\\<\\(\\[\\{\\\\\\^\\-\\=\\$\\!\\|\\]\\}\\)\\?\\*\\+\\.\\>\\\"\\']+)");

  private final ImmutableSet<String> tokenizingPatterns;

  public TokenizingColPatternChecker(Collection<String> fieldNames) {
    this.tokenizingPatterns =
        fieldNames.stream()
            .map(TokenizingColPatternChecker::makeArrayPatternForField)
            .collect(toImmutableSet());
  }

  public static TokenizingColPatternChecker forTransforms(
      Collection<FieldTransformation> transformations) {
    return new TokenizingColPatternChecker(
        transformations.stream()
            .map(FieldTransformation::getFieldsList)
            .flatMap(Collection::stream)
            .map(FieldId::getName)
            .collect(toImmutableSet()));
  }

  public boolean isTokenizeColumn(String columnName) {
    return tokenizingPatterns.stream()
        .map(columnName::matches)
        .filter(Boolean.TRUE::equals)
        .findAny()
        .orElse(Boolean.FALSE);
  }

  public boolean isTokenizeColumn(FieldId fieldId) {
    return isTokenizeColumn(fieldId.getName());
  }

  private static String makeArrayPatternForField(String fieldName) {
    return Splitter.on('.')
        .splitToStream(fieldName)
        .map(part -> REGEX_SPECIAL_CHARS.matcher(part).replaceAll("\\\\$0"))
        .map(part -> part + "(?:\\[\\d+\\])?")
        .collect(Collectors.joining("\\."));
  }
}
