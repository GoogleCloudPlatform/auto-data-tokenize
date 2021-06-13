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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.truth.Correspondence;
import com.google.common.truth.Correspondence.BinaryPredicate;
import com.google.common.truth.Correspondence.DiffFormatter;
import com.google.common.truth.IterableSubject.UsingCorrespondence;
import com.google.privacy.dlp.v2.FieldId;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

public class FieldIdMatchesTokenizeColumns {
  private final String recordIdColumnName;

  public FieldIdMatchesTokenizeColumns(String recordIdColumnName) {
    this.recordIdColumnName = recordIdColumnName;
  }

  public static FieldIdMatchesTokenizeColumns withRecordIdColumn(String recordIdColumn) {
    return new FieldIdMatchesTokenizeColumns(recordIdColumn);
  }

  public UsingCorrespondence<FieldId, TokenizingColPatternChecker> assertExpectedHeadersOnly(
      List<FieldId> headers) {
    return assertThat(headers)
        .comparingElementsUsing(
            Correspondence.from(
                    (BinaryPredicate<FieldId, TokenizingColPatternChecker>)
                        (actual, expected) -> {
                          assertThat(expected).isNotNull();

                          return (actual.getName().equals(recordIdColumnName))
                              || expected.isTokenizeColumn(actual);
                        },
                    "Field Id name array matched:")
                .formattingDiffsUsing(
                    new DiffFormatter<>() {
                      @Nullable
                      @Override
                      public String formatDiff(
                          @Nullable FieldId actual,
                          @Nullable TokenizingColPatternChecker expected) {
                        return String.format(
                            "%s not found in expected Headers",
                            (actual == null) ? null : actual.getName());
                      }
                    }));
  }
}
