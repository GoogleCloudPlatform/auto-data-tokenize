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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.privacy.dlp.v2.FieldId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TokenizingColPatternCheckerTest {

  @Test
  public void isTokenizeColumn_singleLevelArrayElement_isTrue() {
    TokenizingColPatternChecker testChecker =
        new TokenizingColPatternChecker(
            ImmutableList.of(
                "$.level1_array.[\"level1_array_record\"].level2_simple_field.string"));

    assertThat(
            testChecker.isTokenizeColumn(
                FieldId.newBuilder()
                    .setName(
                        "$.level1_array[0].[\"level1_array_record\"].level2_simple_field.string")
                    .build()))
        .isTrue();
  }

  @Test
  public void isTokenizeColumn_singleLevelArrayElementNonMatchingArrayExpansion_isFalse() {
    TokenizingColPatternChecker testChecker =
        new TokenizingColPatternChecker(
            ImmutableList.of(
                "$.level1_array.[\"level1_array_record\"].level2_simple_field.string"));

    assertThat(
            testChecker.isTokenizeColumn(
                FieldId.newBuilder()
                    .setName("$.level1_array[0].[\"level1_array_record\"].level2_array[1].string")
                    .build()))
        .isFalse();
  }

  @Test
  public void isTokenizeColumn_twoLevelArrayElement_isTrue() {
    TokenizingColPatternChecker testChecker =
        new TokenizingColPatternChecker(
            ImmutableList.of("$.level1_array.[\"level1_array_record\"].level2_array.string"));

    assertThat(
            testChecker.isTokenizeColumn(
                FieldId.newBuilder()
                    .setName("$.level1_array[1].[\"level1_array_record\"].level2_array[1].string")
                    .build()))
        .isTrue();
  }
}
