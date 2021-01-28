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

import com.google.common.truth.Fact;
import com.google.common.truth.FailureMetadata;
import com.google.common.truth.Subject;
import com.google.common.truth.Truth;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONException;
import org.json.JSONObject;
import org.skyscreamer.jsonassert.JSONAssert;

public class JsonSubject extends Subject {

  private static final Factory<JsonSubject, Object> JSON_STRING_SUBJECT_FACTORY = JsonSubject::new;
  private final String actualJsonString;

  protected JsonSubject(FailureMetadata metadata, @Nullable Object actualJson) {
    super(metadata, actualJson);
    this.actualJsonString = (actualJson != null) ? actualJson.toString() : null;
  }

  public static JsonSubject assertThat(Object actualJson) {
    return Truth.assertAbout(JSON_STRING_SUBJECT_FACTORY).that(actualJson);
  }

  @Override
  public void isEqualTo(@Nullable Object expectedJson) {
    try {
      String expectedJsonString = null;
      if (actualJsonString == null) {
        super.isEqualTo(expectedJson);
      } else if (expectedJson instanceof String) {
        expectedJsonString = (String) expectedJson;
      } else if (expectedJson instanceof JSONObject) {
        expectedJsonString = expectedJson.toString();
      } else if (expectedJson == null) {
        super.isEqualTo(null);
      } else {
        expectedJsonString = expectedJson.toString();
      }

      JSONAssert.assertEquals(expectedJsonString, actualJsonString, false);
    } catch (JSONException jsonException) {
      failWithoutActual(Fact.fact("jsonException", jsonException));
    }
  }
}
