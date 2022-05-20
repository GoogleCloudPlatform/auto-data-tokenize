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

package com.google.cloud.solutions.autotokenize.encryptors;

import static com.google.common.truth.extensions.proto.ProtoTruth.assertThat;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.KeyMaterialType;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.privacy.dlp.v2.Value;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public final class EncryptingFlatRecordTokenizerTest {

  public static DaeadEncryptingValueTokenizerFactory makeTestEncryptor() {
    return new DaeadEncryptingValueTokenizerFactory(
        TestResourceLoader.classPath().loadAsString("test_encryption_key.json"),
        KeyMaterialType.TINK_GCP_KEYSET_JSON);
  }

  @RunWith(Parameterized.class)
  public static final class ParameterizedValidTest {

    private final FlatRecord testRecord;
    private final FlatRecord expectedEncryptedRecord;
    private final ImmutableSet<String> encryptColumns;

    public ParameterizedValidTest(
        String testConditionName,
        FlatRecord testRecord,
        FlatRecord expectedEncryptedRecord,
        ImmutableSet<String> encryptColumns) {
      this.testRecord = testRecord;
      this.expectedEncryptedRecord = expectedEncryptedRecord;
      this.encryptColumns = encryptColumns;
    }

    @Parameters(name = "{0}")
    public static ImmutableList<Object[]> validParameters() {
      return ImmutableList.<Object[]>builder()
          .add(
              new Object[] {
                "all data types empty encrypt columns, no encryption",
                TestResourceLoader.classPath()
                    .forProto(FlatRecord.class)
                    .loadText("flat_records/simple_field_flat_record.textpb"),
                TestResourceLoader.classPath()
                    .forProto(FlatRecord.class)
                    .loadText("flat_records/simple_field_flat_record.textpb")
                    .toBuilder()
                    .clearFlatKeySchema()
                    .build(),
                ImmutableSet.of("")
              })
          .add(
              new Object[] {
                "all data types, union integer column encrypt, valid",
                TestResourceLoader.classPath()
                    .forProto(FlatRecord.class)
                    .loadText("flat_records/simple_field_flat_record.textpb"),
                TestResourceLoader.classPath()
                    .forProto(FlatRecord.class)
                    .loadText("flat_records/simple_field_flat_record.textpb")
                    .toBuilder()
                    .clearFlatKeySchema()
                    .removeValues("$.cc.long")
                    .putValues(
                        "$.encrypted_cc.string",
                        Value.newBuilder()
                            .setStringValue("AWWfEcxlLFkfwRULhORnAz1dfHOUqVhUwfkUdeB4")
                            .build())
                    .build(),
                ImmutableSet.of("$.kylosample.cc")
              })
          .add(
              new Object[] {
                "all data types, union record sub-field encrypt, valid",
                TestResourceLoader.classPath()
                    .forProto(FlatRecord.class)
                    .loadText("flat_records/array_with_null_union_record_flat_record.textpb"),
                TestResourceLoader.classPath()
                    .forProto(FlatRecord.class)
                    .loadText("flat_records/array_with_null_union_record_flat_record.textpb")
                    .toBuilder()
                    .clearFlatKeySchema()
                    .putValues(
                        "$.contacts[0].[\"contact\"].encrypted_number",
                        Value.newBuilder()
                            .setStringValue("AWWfEczlhPa3K59gdZ/6VXzQMtIim8R81fMxSHv5QGJ8s6A=")
                            .build())
                    .putValues(
                        "$.contacts[1].[\"contact\"].encrypted_number",
                        Value.newBuilder()
                            .setStringValue("AWWfEcx2udPxUo7gKS6cRwscJh/S0JYRS+FvR799vheUAQ8=")
                            .build())
                    .removeValues("$.contacts[0].[\"contact\"].number")
                    .removeValues("$.contacts[1].[\"contact\"].number")
                    .build(),
                ImmutableSet.of("$.kylosample.contacts.contact.number")
              })
          .build();
    }

    @Test
    public void encrypt_valid() {
      assertThat(
              EncryptingFlatRecordTokenizer.withTokenizeSchemaKeys(encryptColumns)
                  .withTokenizerFactory(makeTestEncryptor())
                  .encrypt(testRecord))
          .isEqualTo(expectedEncryptedRecord);
    }
  }
}
