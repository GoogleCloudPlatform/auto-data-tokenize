/*
 * Copyright 2022 Google LLC
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

package com.google.cloud.solutions.autotokenize.encryptors;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.KeyMaterialType;
import com.google.cloud.solutions.autotokenize.encryptors.AesEcbStringValueTokenizer.AesEcbValueTokenizerFactory;
import com.google.cloud.solutions.autotokenize.encryptors.ValueTokenizer.ValueTokenizingException;
import com.google.privacy.dlp.v2.Value;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

@RunWith(Parameterized.class)
public class AesEcbStringValueTokenizerTest {
  private final String keyString;
  private final KeyMaterialType keyMaterialType;
  private final String expectedCipherText;
  private final Value testValue;

  private AesEcbStringValueTokenizer valueTokenizer;

  @Before
  public void makeTokenizer() {
    valueTokenizer =
        new AesEcbValueTokenizerFactory(keyString, keyMaterialType).makeValueTokenizer();
  }

  @Test
  public void encryptTest() throws ValueTokenizingException {
    assertThat(valueTokenizer.encrypt(testValue)).isEqualTo(expectedCipherText);
  }

  @Test
  public void decryptTest() throws ValueTokenizingException {
    assertThat(valueTokenizer.decrypt(expectedCipherText)).isEqualTo(testValue);
  }

  @Test
  public void encryptAndDecryptTest() throws ValueTokenizingException {
    assertThat(valueTokenizer.decrypt(valueTokenizer.encrypt(testValue))).isEqualTo(testValue);
  }

  public AesEcbStringValueTokenizerTest(
      String keyString, String keyMaterialType, String testPlainText, String expectedCipherText) {
    this.keyString = keyString;
    this.keyMaterialType = KeyMaterialType.valueOf(keyMaterialType);
    this.expectedCipherText = expectedCipherText;
    this.testValue = Value.newBuilder().setStringValue(testPlainText).build();
  }

  @Parameters(name = "encrypt({0},{1},{2}) -> {3}")
  public static ImmutableList<Object[]> testParameters() {
    return ImmutableList.of(
        new Object[] {
          "rhrivjuoxkdhxpeggnhwsbvexfhuxzqq",
          "RAW_UTF8_KEY",
          "9171230000",
          "E8joX999ZKGpF/wYq2ppXg=="
        },
        new Object[] {
          "cmhyaXZqdW94a2RoeHBlZ2duaHdzYnZleGZodXh6cXE=",
          "RAW_BASE64_KEY",
          "9171230000",
          "E8joX999ZKGpF/wYq2ppXg=="
        });
  }
}
