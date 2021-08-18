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

package com.google.cloud.solutions.autotokenize.pipeline;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.privacy.dlp.v2.Value;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class RandomColumnarSamplerTest {

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  private final String testCondition;
  private final int sampleSize;
  private final ImmutableList<FlatRecord> flatRecords;
  private final ImmutableMap<String, ImmutableSet<Value>> expectedSplits;

  public RandomColumnarSamplerTest(
      String testCondition,
      int sampleSize,
      ImmutableList<FlatRecord> flatRecords,
      ImmutableMap<String, ImmutableSet<Value>> expectedSplits) {

    this.testCondition = testCondition;
    this.sampleSize = sampleSize;
    this.flatRecords = flatRecords;
    this.expectedSplits = expectedSplits;
  }

  @Test
  public void expand_valid() {
    PCollection<KV<String, Iterable<Value>>> sampledColumns =
        testPipeline.apply(Create.of(flatRecords)).apply(RandomColumnarSampler.any(sampleSize));

    PAssert.that(sampledColumns).satisfies(new ColumnValuesChecker(expectedSplits, sampleSize));

    testPipeline.run();
  }

  private static class ColumnValuesChecker
      implements SerializableFunction<Iterable<KV<String, Iterable<Value>>>, Void> {

    private final ImmutableMap<String, ImmutableSet<Value>> expectedValues;
    private final int maxSamples;

    public ColumnValuesChecker(
        ImmutableMap<String, ImmutableSet<Value>> expectedValues, int maxSamples) {
      this.expectedValues = expectedValues;
      this.maxSamples = maxSamples;
    }

    @Override
    public Void apply(Iterable<KV<String, Iterable<Value>>> columnValuesPcollection) {
      columnValuesPcollection.forEach(
          column -> {
            ImmutableSet<Value> values = ImmutableSet.copyOf(column.getValue());

            if (maxSamples != 0) {
              assertThat(values.size()).isAtMost(maxSamples);
            }

            assertThat(Sets.difference(values, expectedValues.get(column.getKey()))).isEmpty();
          });
      return null;
    }
  }

  @Parameters(name = "{0}")
  public static ImmutableList<Object[]> testParameters() {
    return ImmutableList.<Object[]>builder()
        .add(
            new Object[] {
              /*testCondition=*/ "ArrayElementsAreMerged",
              /*sampleSize=*/ 100,
              /*flatRecords=*/ ImmutableList.of(
                  FlatRecord.newBuilder()
                      .putFlatKeySchema("$.cc[0]", "$.cc")
                      .putFlatKeySchema("$.cc[1]", "$.cc")
                      .putFlatKeySchema("$.cc[2]", "$.cc")
                      .putValues("$.cc[0]", Value.newBuilder().setIntegerValue(1234567890L).build())
                      .putValues("$.cc[1]", Value.newBuilder().setIntegerValue(9876543210L).build())
                      .putValues(
                          "$.cc[2]", Value.newBuilder().setIntegerValue(1212343456567878L).build())
                      .build()),
              /*expectedSplits=*/ ImmutableMap.<String, ImmutableSet<Value>>builder()
                  .put(
                      "$.cc",
                      ImmutableSet.of(
                          Value.newBuilder().setIntegerValue(1234567890L).build(),
                          Value.newBuilder().setIntegerValue(9876543210L).build(),
                          Value.newBuilder().setIntegerValue(1212343456567878L).build()))
                  .build()
            })
        .add(
            new Object[] {
              /*testCondition=*/ "EmptyEmailFieldRemoved",
              /*sampleSize=*/ 100,
              /*flatRecords=*/ ImmutableList.of(
                  FlatRecord.newBuilder()
                      .putFlatKeySchema("$.cc[0]", "$.cc")
                      .putFlatKeySchema("$.cc[1]", "$.cc")
                      .putFlatKeySchema("$.email", "$.email")
                      .putValues("$.cc[0]", Value.newBuilder().setIntegerValue(1234567890L).build())
                      .putValues("$.cc[1]", Value.newBuilder().setIntegerValue(9876543210L).build())
                      .putValues("$.email", Value.getDefaultInstance())
                      .build()),
              /*expectedSplits=*/ ImmutableMap.<String, ImmutableSet<Value>>builder()
                  .put(
                      "$.cc",
                      ImmutableSet.of(
                          Value.newBuilder().setIntegerValue(1234567890L).build(),
                          Value.newBuilder().setIntegerValue(9876543210L).build()))
                  .build()
            })
        .add(
            new Object[] {
              /*testCondition=*/ "ElementsReduced",
              /*sampleSize=*/ 5,
              /*flatRecords=*/ ImmutableList.of(
                  FlatRecord.newBuilder()
                      .putFlatKeySchema("$.cc[0]", "$.cc")
                      .putFlatKeySchema("$.cc[1]", "$.cc")
                      .putFlatKeySchema("$.cc[2]", "$.cc")
                      .putFlatKeySchema("$.cc[3]", "$.cc")
                      .putFlatKeySchema("$.cc[4]", "$.cc")
                      .putFlatKeySchema("$.cc[5]", "$.cc")
                      .putFlatKeySchema("$.cc[6]", "$.cc")
                      .putFlatKeySchema("$.cc[7]", "$.cc")
                      .putFlatKeySchema("$.cc[8]", "$.cc")
                      .putFlatKeySchema("$.cc[9]", "$.cc")
                      .putValues("$.cc[0]", Value.newBuilder().setIntegerValue(123).build())
                      .putValues("$.cc[1]", Value.newBuilder().setIntegerValue(456).build())
                      .putValues("$.cc[2]", Value.newBuilder().setIntegerValue(789).build())
                      .putValues("$.cc[3]", Value.newBuilder().setIntegerValue(987).build())
                      .putValues("$.cc[4]", Value.newBuilder().setIntegerValue(654).build())
                      .putValues("$.cc[5]", Value.newBuilder().setIntegerValue(321).build())
                      .putValues("$.cc[6]", Value.newBuilder().setIntegerValue(1234).build())
                      .putValues("$.cc[7]", Value.newBuilder().setIntegerValue(5678).build())
                      .putValues("$.cc[8]", Value.newBuilder().setIntegerValue(91011).build())
                      .putValues("$.cc[9]", Value.newBuilder().setIntegerValue(111213).build())
                      .build()),
              /*expectedSplits=*/ ImmutableMap.<String, ImmutableSet<Value>>builder()
                  .put(
                      "$.cc",
                      ImmutableSet.of(
                          Value.newBuilder().setIntegerValue(123).build(),
                          Value.newBuilder().setIntegerValue(456).build(),
                          Value.newBuilder().setIntegerValue(789).build(),
                          Value.newBuilder().setIntegerValue(987).build(),
                          Value.newBuilder().setIntegerValue(654).build(),
                          Value.newBuilder().setIntegerValue(321).build(),
                          Value.newBuilder().setIntegerValue(1234).build(),
                          Value.newBuilder().setIntegerValue(5678).build(),
                          Value.newBuilder().setIntegerValue(91011).build(),
                          Value.newBuilder().setIntegerValue(111213).build()))
                  .build()
            })
        .add(
            new Object[] {
              /*testCondition=*/ "numItemsLessThanSampleSize",
              /*sampleSize=*/ 100,
              /*flatRecords=*/ TestResourceLoader.classPath()
                  .forProto(FlatRecord.class)
                  .loadAllTextFiles(
                      ImmutableList.of(
                          "flat_records/userdata_avro/record-1.textpb",
                          "flat_records/userdata_avro/record-2.textpb",
                          "flat_records/userdata_avro/record-cc-null.textpb")),
              /*expectedSplits=*/ ImmutableMap.<String, ImmutableSet<Value>>builder()
                  .put(
                      "$.kylosample.ip_address",
                      ImmutableSet.of(
                          Value.newBuilder().setStringValue("1.197.201.2").build(),
                          Value.newBuilder().setStringValue("7.161.136.94").build(),
                          Value.newBuilder().setStringValue("218.111.175.34").build()))
                  .put(
                      "$.kylosample.comments",
                      ImmutableSet.of(
                          Value.newBuilder().setStringValue("1E+02").build(),
                          Value.newBuilder().setStringValue("").build(),
                          Value.newBuilder().setStringValue("").build()))
                  .put(
                      "$.kylosample.email",
                      ImmutableSet.of(
                          Value.newBuilder().setStringValue("afreeman1@is.gd").build(),
                          Value.newBuilder().setStringValue("ajordan0@com.com").build(),
                          Value.newBuilder().setStringValue("emorgan2@altervista.org").build()))
                  .put(
                      "$.kylosample.birthdate",
                      ImmutableSet.of(
                          Value.newBuilder().setStringValue("1/16/1968").build(),
                          Value.newBuilder().setStringValue("3/8/1971").build(),
                          Value.newBuilder().setStringValue("2/1/1960").build()))
                  .put(
                      "$.kylosample.title",
                      ImmutableSet.of(
                          Value.newBuilder().setStringValue("Accountant IV").build(),
                          Value.newBuilder().setStringValue("Internal Auditor").build(),
                          Value.newBuilder().setStringValue("Structural Engineer").build()))
                  .put(
                      "$.kylosample.gender",
                      ImmutableSet.of(
                          Value.newBuilder().setStringValue("Female").build(),
                          Value.newBuilder().setStringValue("Male").build(),
                          Value.newBuilder().setStringValue("Female").build()))
                  .put(
                      "$.kylosample.registration_dttm",
                      ImmutableSet.of(
                          Value.newBuilder().setStringValue("2016-02-03T07:55:29Z").build(),
                          Value.newBuilder().setStringValue("2016-02-03T01:09:31Z").build(),
                          Value.newBuilder().setStringValue("2016-02-03T17:04:03Z").build()))
                  .put(
                      "$.kylosample.cc",
                      ImmutableSet.of(
                          Value.newBuilder().setIntegerValue(6759521864920116L).build(),
                          Value.newBuilder().setIntegerValue(6767119071901597L).build()))
                  .put(
                      "$.kylosample.salary",
                      ImmutableSet.of(
                          Value.newBuilder().setFloatValue(49756.53).build(),
                          Value.newBuilder().setFloatValue(150280.17).build(),
                          Value.newBuilder().setFloatValue(144972.51).build()))
                  .put(
                      "$.kylosample.first_name",
                      ImmutableSet.of(
                          Value.newBuilder().setStringValue("Amanda").build(),
                          Value.newBuilder().setStringValue("Evelyn").build(),
                          Value.newBuilder().setStringValue("Albert").build()))
                  .put(
                      "$.kylosample.last_name",
                      ImmutableSet.of(
                          Value.newBuilder().setStringValue("Freeman").build(),
                          Value.newBuilder().setStringValue("Jordan").build(),
                          Value.newBuilder().setStringValue("Morgan").build()))
                  .put(
                      "$.kylosample.country",
                      ImmutableSet.of(
                          Value.newBuilder().setStringValue("Indonesia").build(),
                          Value.newBuilder().setStringValue("Canada").build(),
                          Value.newBuilder().setStringValue("Russia").build()))
                  .put(
                      "$.kylosample.id",
                      ImmutableSet.of(
                          Value.newBuilder().setIntegerValue(1).build(),
                          Value.newBuilder().setIntegerValue(2).build(),
                          Value.newBuilder().setIntegerValue(3).build()))
                  .build()
            })
        .add(
            new Object[] {
              /*testCondition=*/ "usesGroupIntoBatches",
              /*sampleSize=*/ 0,
              /*flatRecords=*/ ImmutableList.of(
                  FlatRecord.newBuilder()
                      .putFlatKeySchema("$.cc[0]", "$.cc")
                      .putFlatKeySchema("$.cc[1]", "$.cc")
                      .putFlatKeySchema("$.email", "$.email")
                      .putValues("$.cc[0]", Value.newBuilder().setIntegerValue(1234567890L).build())
                      .putValues("$.cc[1]", Value.newBuilder().setIntegerValue(9876543210L).build())
                      .putValues(
                          "$.email", Value.newBuilder().setStringValue("email@domain.ext").build())
                      .build(),
                  FlatRecord.newBuilder()
                      .putFlatKeySchema("$.cc[0]", "$.cc")
                      .putFlatKeySchema("$.cc[1]", "$.cc")
                      .putFlatKeySchema("$.email", "$.email")
                      .putValues("$.cc[0]", Value.newBuilder().setIntegerValue(4567890L).build())
                      .putValues("$.cc[1]", Value.newBuilder().setIntegerValue(91919989L).build())
                      .putValues(
                          "$.email",
                          Value.newBuilder().setStringValue("email2@domain2.ext2").build())
                      .build()),
              /*expectedSplits=*/ ImmutableMap.<String, ImmutableSet<Value>>builder()
                  .put(
                      "$.cc",
                      ImmutableSet.of(
                          Value.newBuilder().setIntegerValue(1234567890L).build(),
                          Value.newBuilder().setIntegerValue(9876543210L).build(),
                          Value.newBuilder().setIntegerValue(4567890L).build(),
                          Value.newBuilder().setIntegerValue(91919989L).build()))
                  .put(
                      "$.email",
                      ImmutableSet.of(
                          Value.newBuilder().setStringValue("email@domain.ext").build(),
                          Value.newBuilder().setStringValue("email2@domain2.ext2").build()))
                  .build()
            })
        .build();
  }
}
