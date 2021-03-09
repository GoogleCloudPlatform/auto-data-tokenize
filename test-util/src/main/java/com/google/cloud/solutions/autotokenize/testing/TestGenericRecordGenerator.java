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


import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public final class TestGenericRecordGenerator {

  public static final String SCHEMA_STRING =
      "{"
          + "\"type\":\"record\", "
          + "\"name\":\"testrecord\","
          + "\"fields\":["
          + "    {\"name\":\"name\",\"type\":\"string\"},"
          + "    {\"name\":\"id\",\"type\":\"string\"}"
          + "  ]"
          + "}";

  public static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

  private static final String[] SCIENTISTS =
      new String[] {
        "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie",
        "Faraday", "Newton", "Bohr", "Galilei", "Maxwell"
      };

  public static ImmutableList<GenericRecord> generateGenericRecors(int count) {

    ImmutableList.Builder<GenericRecord> recordsList = ImmutableList.builder();

    for (int i = 0; i < count; i++) {
      int index = i % SCIENTISTS.length;
      GenericRecord record =
          new GenericRecordBuilder(SCHEMA)
              .set("name", SCIENTISTS[index] + "-" + i)
              .set("id", Integer.toString(i))
              .build();
      recordsList.add(record);
    }
    return recordsList.build();
  }

  private TestGenericRecordGenerator() {}
}
