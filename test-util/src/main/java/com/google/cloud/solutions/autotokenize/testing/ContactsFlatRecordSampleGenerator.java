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


import com.google.api.client.util.Maps;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.common.collect.ImmutableList;
import com.google.privacy.dlp.v2.Value;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;

public class ContactsFlatRecordSampleGenerator {

  private final boolean omitId;
  private final Random random;

  private ContactsFlatRecordSampleGenerator(boolean omitId) {
    this.omitId = omitId;
    this.random = new Random();
  }

  public static ContactsFlatRecordSampleGenerator create() {
    return new ContactsFlatRecordSampleGenerator(/*omitId=*/ false);
  }

  public static ContactsFlatRecordSampleGenerator withOmitRecordId() {
    return new ContactsFlatRecordSampleGenerator(/*omitId=*/ true);
  }

  public ImmutableList<FlatRecord> buildContactRecords(int count) {

    ImmutableList.Builder<FlatRecord> recordListBuilder = ImmutableList.builder();

    for (int i = 0; i < count; i++) {

      HashMap<String, Value> valuesMap = Maps.newHashMap();
      HashMap<String, String> flatKeyMap = Maps.newHashMap();

      valuesMap.put("$.name", Value.newBuilder().setStringValue(randomName()).build());
      flatKeyMap.put("$.name", "$.name");

      final int numbers = new Random().nextInt(10);
      for (int n = 0; n < numbers; n++) {
        String key = "$.contacts[" + n + "].contact.number";
        flatKeyMap.put(key, "$.contacts.contact.number");

        valuesMap.put(key, Value.newBuilder().setStringValue(randomPhoneNumber(10)).build());
      }

      final int emails = new Random().nextInt(5);
      for (int n = 0; n < emails; n++) {
        String key = "$.emails[" + n + "]";
        flatKeyMap.put(key, "$.emails");

        valuesMap.put(key, Value.newBuilder().setStringValue(randomName()).build());
      }

      recordListBuilder.add(
          flatRecordBuilder().putAllValues(valuesMap).putAllFlatKeySchema(flatKeyMap).build());
    }

    return recordListBuilder.build();
  }

  private FlatRecord.Builder flatRecordBuilder() {
    FlatRecord.Builder builder = FlatRecord.newBuilder();
    return (omitId) ? builder : builder.setRecordId(UUID.randomUUID().toString());
  }

  private String randomName() {
    StringBuilder stringBuilder = new StringBuilder();

    random.ints(random.nextInt(26), 'A', 'Z').forEach(i -> stringBuilder.append((char) i));

    return stringBuilder.toString();
  }

  private String randomPhoneNumber(int numLength) {
    StringBuilder builder = new StringBuilder();

    random.ints(numLength, '0', '9').forEach(i -> builder.append((char) i));

    return builder.toString();
  }
}
