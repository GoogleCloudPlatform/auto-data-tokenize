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

package com.google.cloud.solutions.autotokenize.dlp;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnTransform;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.common.RecordFlattener;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.cloud.solutions.autotokenize.testing.stubs.dlp.Base64EncodingDlpStub;
import com.google.cloud.solutions.autotokenize.testing.stubs.dlp.StubbingDlpClientFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.extensions.proto.ProtoTruth;
import com.google.privacy.dlp.v2.CryptoDeterministicConfig;
import com.google.privacy.dlp.v2.CryptoKey;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.UnwrappedCryptoKey;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.Base64;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BatchAndDlpDeIdRecordsTest {

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void expand_valid() {

    ImmutableList<FlatRecord> expectedBase64EncodedContacts =
        TestResourceLoader.classPath()
            .forProto(FlatRecord.class)
            .loadAllTextFiles(
                ImmutableList.of(
                    "avro_records/contacts_schema/jane_doe_contact_number_base64_avro_record.textpb",
                    "avro_records/contacts_schema/john_doe_contact_number_base64_avro_record.textpb"));

    PCollection<FlatRecord> tokenizedRecords =
        testPipeline
            .apply(Create.of(CONTACT_RECORDS))
            .apply(
                BatchAndDlpDeIdRecords.withEncryptConfig(NUMBER_TOKENIZE_CONFIG)
                    .withDlpProjectId("dlp-test-project")
                    .withDlpClientFactory(
                        new StubbingDlpClientFactory(
                            new Base64EncodingDlpStub(
                                PartialColumnBatchAccumulator.RECORD_ID_COLUMN_NAME,
                                ImmutableList.of(
                                    "$.contacts[1].[\"contact\"].number",
                                    "$.contacts[0].[\"contact\"].number"),
                                "dlp-test-project"))));

    PAssert.that(tokenizedRecords)
        .satisfies(new FlatRecordCheckerWithoutRecordIds(expectedBase64EncodedContacts));
    testPipeline.run().waitUntilFinish();
  }

  private static final class FlatRecordCheckerWithoutRecordIds
      extends SimpleFunction<Iterable<FlatRecord>, Void> implements Serializable {

    private final ImmutableList<FlatRecord> expectedRecords;

    public FlatRecordCheckerWithoutRecordIds(ImmutableList<FlatRecord> expectedRecords) {
      this.expectedRecords = expectedRecords;
    }

    @Override
    public Void apply(Iterable<FlatRecord> input) {

      ProtoTruth.assertThat(input)
          .ignoringFields(FlatRecord.RECORD_ID_FIELD_NUMBER)
          .containsExactlyElementsIn(expectedRecords);
      return null;
    }
  }

  private static final ImmutableList<FlatRecord> CONTACT_RECORDS =
      TestResourceLoader.classPath()
          .forAvro()
          .withSchemaFile(
              "avro_records/contacts_schema/person_name_union_null_long_contact_schema.json")
          .loadAllRecords(
              "avro_records/contacts_schema/john_doe_contact_plain_avro_record.json",
              "avro_records/contacts_schema/jane_doe_contact_plain_avro_record.json")
          .stream()
          .map(RecordFlattener.forGenericRecord()::flatten)
          .collect(toImmutableList());

  private static final FlatRecord JANE_DOE_CONTACT =
      RecordFlattener.forGenericRecord()
          .flatten(
              TestResourceLoader.classPath()
                  .forAvro()
                  .withSchemaFile(
                      "avro_records/contacts_schema/person_name_union_null_long_contact_schema.json")
                  .loadRecord(
                      "avro_records/contacts_schema/jane_doe_contact_plain_avro_record.json"));

  private static final PrimitiveTransformation CRYPTO_UNWRAPPED_TRANSFORM =
      PrimitiveTransformation.newBuilder()
          .setCryptoDeterministicConfig(
              CryptoDeterministicConfig.newBuilder()
                  .setCryptoKey(
                      CryptoKey.newBuilder()
                          .setUnwrapped(
                              UnwrappedCryptoKey.newBuilder()
                                  .setKey(
                                      ByteString.copyFrom(
                                          Base64.getDecoder()
                                              .decode(
                                                  "QiZFKUhATWNRZlRqV21acTR0N3cheiVDKkYtSmFOZFI="))))))
          .build();

  private static final DlpEncryptConfig NUMBER_TOKENIZE_CONFIG =
      DlpEncryptConfig.newBuilder()
          .addTransforms(
              ColumnTransform.newBuilder()
                  .setColumnId("$.contact_records.contacts.contact.number")
                  .setTransform(CRYPTO_UNWRAPPED_TRANSFORM))
          .build();
}
