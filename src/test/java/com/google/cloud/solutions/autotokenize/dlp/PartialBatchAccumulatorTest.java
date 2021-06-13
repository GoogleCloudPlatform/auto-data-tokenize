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
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnTransform;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.common.DeidentifyColumns;
import com.google.cloud.solutions.autotokenize.common.RecordFlattener;
import com.google.cloud.solutions.autotokenize.dlp.PartialBatchAccumulator.BatchPartialColumnDlpTable;
import com.google.cloud.solutions.autotokenize.testing.FieldIdMatchesTokenizeColumns;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.cloud.solutions.autotokenize.testing.TokenizingColPatternChecker;
import com.google.common.collect.ImmutableList;
import com.google.privacy.dlp.v2.CryptoDeterministicConfig;
import com.google.privacy.dlp.v2.CryptoKey;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.UnwrappedCryptoKey;
import com.google.privacy.dlp.v2.Value;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PartialBatchAccumulatorTest {

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

  @Test
  public void addElement_noRecordId_throwsIllegalArgumentExecption() {
    PartialBatchAccumulator accumulator =
        PartialBatchAccumulator.withConfig(NUMBER_TOKENIZE_CONFIG);

    assertThrows(IllegalArgumentException.class, () -> accumulator.addElement(JANE_DOE_CONTACT));
  }

  @Test
  public void batch_arrayFields_deidConfigContainsOnlyFieldReference() {
    PartialBatchAccumulator accumulator =
        PartialBatchAccumulator.withConfig(
            DlpEncryptConfig.newBuilder()
                .addTransforms(
                    ColumnTransform.newBuilder()
                        .setColumnId("$.multi_level_arrays.simple_field1")
                        .setTransform(CRYPTO_UNWRAPPED_TRANSFORM))
                .addTransforms(
                    ColumnTransform.newBuilder()
                        .setColumnId(
                            "$.multi_level_arrays.level1_array.level1_array_record.level2_simple_field")
                        .setTransform(CRYPTO_UNWRAPPED_TRANSFORM))
                .addTransforms(
                    ColumnTransform.newBuilder()
                        .setColumnId(
                            "$.multi_level_arrays.level1_array.level1_array_record.level2_array")
                        .setTransform(CRYPTO_UNWRAPPED_TRANSFORM))
                .build());

    FlatRecord record =
        RecordFlattener.forGenericRecord()
            .flatten(
                TestResourceLoader.classPath()
                    .forAvro()
                    .withSchemaFile(
                        "avro_records/records_with_two_levels_of_arrays/two_level_arrays_schema.avsc")
                    .loadRecord(
                        "avro_records/records_with_two_levels_of_arrays/simple_two_level_array_record.json"));

    accumulator.addElement(record.toBuilder().setRecordId(UUID.randomUUID().toString()).build());

    BatchPartialColumnDlpTable batch = accumulator.makeBatch();

    ImmutableList<FieldId> deidConfigTokenizeFields =
        batch
            .get()
            .getDeidentifyConfig()
            .getRecordTransformations()
            .getFieldTransformationsList()
            .stream()
            .map(FieldTransformation::getFieldsList)
            .flatMap(List::stream)
            .collect(toImmutableList());

    assertThat(deidConfigTokenizeFields)
        .containsExactlyElementsIn(
            DeidentifyColumns.fieldIdsFor(
                ImmutableList.of(
                    "$.simple_field1",
                    "$.level1_array.[\"level1_array_record\"].level2_simple_field.string",
                    "$.level1_array.[\"level1_array_record\"].level2_array.string")));
  }

  @Test
  public void batch_arrayFields_itemTableContainsFlattenedEntries() {
    PartialBatchAccumulator accumulator =
        PartialBatchAccumulator.withConfig(
            DlpEncryptConfig.newBuilder()
                .addTransforms(
                    ColumnTransform.newBuilder()
                        .setColumnId("$.multi_level_arrays.simple_field1")
                        .setTransform(CRYPTO_UNWRAPPED_TRANSFORM))
                .addTransforms(
                    ColumnTransform.newBuilder()
                        .setColumnId(
                            "$.multi_level_arrays.level1_array.level1_array_record.level2_simple_field")
                        .setTransform(CRYPTO_UNWRAPPED_TRANSFORM))
                .addTransforms(
                    ColumnTransform.newBuilder()
                        .setColumnId(
                            "$.multi_level_arrays.level1_array.level1_array_record.level2_array")
                        .setTransform(CRYPTO_UNWRAPPED_TRANSFORM))
                .build());

    FlatRecord record =
        RecordFlattener.forGenericRecord()
            .flatten(
                TestResourceLoader.classPath()
                    .forAvro()
                    .withSchemaFile(
                        "avro_records/records_with_two_levels_of_arrays/two_level_arrays_schema.avsc")
                    .loadRecord(
                        "avro_records/records_with_two_levels_of_arrays/simple_two_level_array_record.json"));

    accumulator.addElement(record.toBuilder().setRecordId(UUID.randomUUID().toString()).build());

    BatchPartialColumnDlpTable batch = accumulator.makeBatch();

    assertThat(batch.get().getTable().getHeadersList())
        .containsExactlyElementsIn(
            DeidentifyColumns.fieldIdsFor(
                ImmutableList.of(
                    "__AUTOTOKENIZE__RECORD_ID__",
                    "$.simple_field1",
                    "$.level1_array[0].[\"level1_array_record\"].level2_simple_field.string",
                    "$.level1_array[1].[\"level1_array_record\"].level2_array[1].string",
                    "$.level1_array[0].[\"level1_array_record\"].level2_array[0].string",
                    "$.level1_array[0].[\"level1_array_record\"].level2_array[1].string",
                    "$.level1_array[1].[\"level1_array_record\"].level2_simple_field.string",
                    "$.level1_array[1].[\"level1_array_record\"].level2_array[0].string")));
  }

  @Test
  public void batch_nullableUnionField_valid() {
    PartialBatchAccumulator accumulator =
        PartialBatchAccumulator.withConfig(
            TestResourceLoader.classPath()
                .forProto(DlpEncryptConfig.class)
                .loadJson("email_cc_dlp_encrypt_config.json"));

    var flatRecords =
        TestResourceLoader.classPath()
            .forAvro()
            .withSchemaFile("avro_records/userdata_records/schema.json")
            .loadAllRecords(
                "avro_records/userdata_records/record-2.json",
                "avro_records/userdata_records/record-3-cc-null.json")
            .stream()
            .map(RecordFlattener.forGenericRecord()::flatten)
            .map(record -> record.toBuilder().setRecordId(UUID.randomUUID().toString()).build())
            .collect(toImmutableList());

    accumulator.addAllElements(flatRecords);

    BatchPartialColumnDlpTable batch = accumulator.makeBatch();

    FieldIdMatchesTokenizeColumns.withRecordIdColumn("__AUTOTOKENIZE__RECORD_ID__")
        .assertExpectedHeadersOnly(batch.get().getTable().getHeadersList())
        .contains(TokenizingColPatternChecker.of("$.email", "$.cc"));
  }

  @Test
  public void addElement_exceedsSize_returnsFalse() {
    PartialBatchAccumulator accumulator =
        PartialBatchAccumulator.withConfig(
            NUMBER_TOKENIZE_CONFIG.toBuilder()
                .addTransforms(
                    ColumnTransform.newBuilder()
                        .setColumnId("$.name")
                        .setTransform(CRYPTO_UNWRAPPED_TRANSFORM)
                        .build())
                .build());

    Value testValue = get1KByteString();
    FlatRecord testRecord =
        FlatRecord.newBuilder()
            .setRecordId("!24")
            .putFlatKeySchema("$.name", "$.name")
            .putValues("$.name", testValue)
            .build();

    // Fill the accumulator till its full.
    while (accumulator.addElement(testRecord))
      ;

    assertThat(accumulator.addElement(testRecord)).isFalse();
    assertThat(
            accumulator.makeBatch().get().getTable().getSerializedSize()
                + testValue.getSerializedSize())
        .isGreaterThan(PartialBatchAccumulator.MAX_DLP_PAYLOAD_SIZE_BYTES);
  }

  private static Value get1KByteString() {
    byte[] randomBytes = new byte[2500];
    new Random().nextBytes(randomBytes);
    return Value.newBuilder()
        .setStringValue(new String(randomBytes, StandardCharsets.UTF_8))
        .build();
  }
}
