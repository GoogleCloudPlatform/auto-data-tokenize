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

import static org.junit.Assert.assertThrows;

import com.google.cloud.solutions.autotokenize.common.JsonConvertor.JsonConversionException;
import com.google.cloud.solutions.autotokenize.testing.JavaBean;
import com.google.cloud.solutions.autotokenize.testing.JsonSubject;
import com.google.cloud.solutions.autotokenize.testing.NonJavaBeanClass;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.common.truth.Truth;
import com.google.common.truth.extensions.proto.ProtoTruth;
import com.google.privacy.dlp.v2.Value;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public final class JsonConvertorTest {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  @RunWith(Parameterized.class)
  public static class AsJsonStringTest {

    private final Object input;
    private final String expectedJson;

    public AsJsonStringTest(String testConditionName, Object input, String expectedJson) {
      this.input = input;
      this.expectedJson = expectedJson;
    }

    @Test
    public void asJsonString_valid() {
      var jsonValue = JsonConvertor.asJsonString(input);
      logger.atInfo().log("jsonValue: %s", jsonValue);
      JsonSubject.assertThat(jsonValue).isEqualTo(expectedJson);
    }

    @Parameters(name = "{0}")
    public static ImmutableList<Object[]> asJsonStringTestParameters() {
      return ImmutableList.<Object[]>builder()
          .add(new Object[] {"null object, empty string", null, ""})
          .add(
              new Object[] {
                "ProtoObject uses JsonPrinter",
                Value.newBuilder().setStringValue("sample_text").build(),
                "{\"stringValue\" : \"sample_text\"}"
              })
          .add(
              new Object[] {
                "List<String> -> JSONArray",
                ImmutableList.of("value1", "value2"),
                "[\"value1\", \"value2\"]"
              })
          .add(
              new Object[] {
                "Set<String> -> JSONArray",
                ImmutableSet.of("value1", "value2"),
                "[\"value1\", \"value2\"]"
              })
          .add(
              new Object[] {
                "List<ProtoObject> -> JSONArray[ProtoJson]",
                ImmutableList.of(
                    Value.newBuilder().setStringValue("value1").build(),
                    Value.newBuilder().setStringValue("value2").build()),
                "[{\"stringValue\" : \"value1\"}, {\"stringValue\" : \"value2\"}]"
              })
          .add(
              new Object[] {
                "Map<String, String> -> JSON",
                ImmutableMap.of("k1", "v1", "k2", "v2"),
                "{\"k1\": \"v1\", \"k2\": \"v2\"}"
              })
          .add(new Object[] {"empty map -> empty jon", ImmutableMap.of(), "{}"})
          .add(
              new Object[] {
                "JavaBean -> JSON",
                JavaBean.create("my-string", 123),
                "{\"string\": \"my-string\", \"integer\": 123 }"
              })
          .add(
              new Object[] {
                "NonJavaBean -> emptyString", new NonJavaBeanClass("my-string", 123), ""
              })
          .build();
    }
  }

  @RunWith(JUnit4.class)
  public static class ProtoConversionTest {

    @Test
    public void parseJson_valueJson_valid() {
      var protoObj = JsonConvertor.parseJson("{\"stringValue\" : \"sample_text\"}", Value.class);

      ProtoTruth.assertThat(protoObj)
          .ignoringRepeatedFieldOrder()
          .isEqualTo(Value.newBuilder().setStringValue("sample_text").build());
    }

    @Test
    public void parseAsList_listJson_valid() {

      var protoObjList =
          JsonConvertor.parseAsList(
              ImmutableList.of(
                  "{\"stringValue\" : \"sample_text\"}", "{\"stringValue\" : \"sample_text2\"}"),
              Value.class);

      Truth.assertThat(protoObjList)
          .isEqualTo(
              ImmutableList.of(
                  Value.newBuilder().setStringValue("sample_text").build(),
                  Value.newBuilder().setStringValue("sample_text2").build()));
    }

    @Test
    public void parseAsList_jsonArray_valid() {

      var protoObjList =
          JsonConvertor.parseAsList(
              "[{\"stringValue\" : \"sample_text\"},{\"stringValue\" : \"sample_text2\"}]",
              Value.class);

      Truth.assertThat(protoObjList)
          .isEqualTo(
              ImmutableList.of(
                  Value.newBuilder().setStringValue("sample_text").build(),
                  Value.newBuilder().setStringValue("sample_text2").build()));
    }
  }

  @RunWith(JUnit4.class)
  public static class JsonAvroConversionTest {

    private static final String TEST_SCHEMA =
        TestResourceLoader.classPath()
            .loadAsString(
                "avro_records/contacts_schema/person_name_union_null_long_contact_schema.json");

    @Test
    public void convertJsonToAvro_valid() {
      var testAvroJson =
          TestResourceLoader.classPath()
              .loadAsString("avro_records/contacts_schema/john_doe_contact_plain_avro_record.json");

      var genericRecord = JsonConvertor.convertJsonToAvro(TEST_SCHEMA, testAvroJson);

      JsonSubject.assertThat(JsonConvertor.convertRecordToJson(genericRecord))
          .isEqualTo(testAvroJson);
    }

    @Test
    public void convertJsonToAvro_inValid_throwsException() {
      assertThrows(
          JsonConversionException.class,
          () -> JsonConvertor.convertJsonToAvro(TEST_SCHEMA, "{\"name\": \"test1\"}"));
    }
  }
}
