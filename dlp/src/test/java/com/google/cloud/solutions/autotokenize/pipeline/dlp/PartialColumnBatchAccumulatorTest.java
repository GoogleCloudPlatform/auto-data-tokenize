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

package com.google.cloud.solutions.autotokenize.pipeline.dlp;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnTransform;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.PartialColumnDlpTable;
import com.google.cloud.solutions.autotokenize.common.DeidentifyColumns;
import com.google.cloud.solutions.autotokenize.testing.ContactsFlatRecordSampleGenerator;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.extensions.proto.ProtoTruth;
import com.google.privacy.dlp.v2.CryptoDeterministicConfig;
import com.google.privacy.dlp.v2.CryptoKey;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.KmsWrappedCryptoKey;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.RecordTransformations;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class PartialColumnBatchAccumulatorTest {

  private static final PrimitiveTransformation CONTACT_TRANSFORM =
    PrimitiveTransformation.newBuilder()
      .setCryptoDeterministicConfig(
        CryptoDeterministicConfig.newBuilder()
          .setCryptoKey(
            CryptoKey.newBuilder()
              .setKmsWrapped(
                KmsWrappedCryptoKey.newBuilder()
                  .setWrappedKey(ByteString.copyFromUtf8("CONTACT_WRAPPED_KEY"))
                  .setCryptoKeyName("kms/key/name/id1")
                  .build())
              .build())
          .build())
      .build();

  private static final PrimitiveTransformation EMAIL_TRANSFORM =
    PrimitiveTransformation.newBuilder()
      .setCryptoDeterministicConfig(
        CryptoDeterministicConfig.newBuilder()
          .setCryptoKey(
            CryptoKey.newBuilder()
              .setKmsWrapped(
                KmsWrappedCryptoKey.newBuilder()
                  .setWrappedKey(ByteString.copyFromUtf8("EMAIL_WRAPPED_KEY"))
                  .setCryptoKeyName("kms/key/name/id2")
                  .build())
              .build())
          .build())
      .build();

  private static final DlpEncryptConfig TEST_ENCRYPT_CONFIG =
    DlpEncryptConfig.newBuilder()
      .addTransforms(
        ColumnTransform.newBuilder()
          .setColumnId("$.contacts.contact.number")
          .setTransform(CONTACT_TRANSFORM)
          .build())
      .addTransforms(
        ColumnTransform.newBuilder()
          .setColumnId("$.emails")
          .setTransform(EMAIL_TRANSFORM)
          .build())
      .build();

  private ContactsFlatRecordSampleGenerator sampleGenerator;
  private FlatRecord singleTestRecord;
  private FlatRecord getSingleTestRecordWithoutId;
  private ImmutableList<FlatRecord> fiftySampleRecords;

  @Before
  public void setUpSampleGenerator() {
    sampleGenerator = ContactsFlatRecordSampleGenerator.create();
    singleTestRecord = sampleGenerator.buildContactRecords(1).get(0);

    getSingleTestRecordWithoutId = ContactsFlatRecordSampleGenerator.withOmitRecordId().buildContactRecords(1).get(0);

    fiftySampleRecords = sampleGenerator.buildContactRecords(50);
  }

  @Test
  public void addElement_empty_true() {
    assertThat(PartialColumnBatchAccumulator.withConfig(TEST_ENCRYPT_CONFIG).addElement(singleTestRecord)).isTrue();
  }

  @Test
  public void addElement_noStorage_false() {
    PartialColumnBatchAccumulator accumulator =
      PartialColumnBatchAccumulator
        .withConfig(TEST_ENCRYPT_CONFIG)
        .withMaxPayloadSize(1)
        .withMaxCellCount(50);

    assertThat(accumulator.addElement(singleTestRecord)).isFalse();
  }

  @Test
  public void addElement_noCells_false() {
    PartialColumnBatchAccumulator accumulator =
      PartialColumnBatchAccumulator
        .withConfig(TEST_ENCRYPT_CONFIG)
        .withMaxPayloadSize(100)
        .withMaxCellCount(1);

    assertThat(accumulator.addElement(singleTestRecord)).isFalse();
  }

  @Test
  public void addElement_emptyRecordId_throwsException() {
    PartialColumnBatchAccumulator accumulator = PartialColumnBatchAccumulator.withConfig(TEST_ENCRYPT_CONFIG);

    IllegalArgumentException iaex =
      assertThrows(IllegalArgumentException.class, () -> accumulator.addElement(getSingleTestRecordWithoutId));

    assertThat(iaex).hasMessageThat().contains("Provide FlatRecord with unique RecordId, found empty");
  }

  @Test
  public void addElement_doesNotAddAfterFalse() {
    PartialColumnBatchAccumulator accumulator =
      PartialColumnBatchAccumulator.withConfig(TEST_ENCRYPT_CONFIG)
        .withMaxPayloadSize(50000)
        .withMaxCellCount(1000);

    int falseRecordCount = addTestRecordsToAccumulator(accumulator, sampleGenerator.buildContactRecords(2000));

    assertThat(accumulator.makeBatch().elementsCount()).isEqualTo(falseRecordCount);
  }

  @Test
  public void makeBatch_customRecordIdColumnName_batchTableUsesProvidedName() {
    PartialColumnBatchAccumulator accumulator =
      PartialColumnBatchAccumulator.withConfig(TEST_ENCRYPT_CONFIG).withRecordIdColumnName("MY_RECORD_ID");
    accumulator.addElement(singleTestRecord);

    PartialColumnDlpTable batchTable = accumulator.makeBatch().get();

    ImmutableList<String> batchHeaders = DeidentifyColumns.columnNamesIn(batchTable.getTable());
    assertThat(batchHeaders).contains("MY_RECORD_ID");
    assertThat(batchHeaders).doesNotContain(PartialColumnBatchAccumulator.RECORD_ID_COLUMN_NAME);
  }

  @Test
  public void makeBatch_customRecordIdColumnName() throws IOException {
    PartialColumnBatchAccumulator accumulator =
      PartialColumnBatchAccumulator.withConfig(TEST_ENCRYPT_CONFIG).withRecordIdColumnName("MY_RECORD_ID");
    accumulator.addElement(singleTestRecord);

    PartialColumnDlpTable batchedTable = accumulator.makeBatch().get();

    assertThat(batchedTable.getRecordIdColumnName()).isEqualTo("MY_RECORD_ID");
    assertThat(batchedTable.getRecordIdColumnName()).isNotEqualTo(PartialColumnBatchAccumulator.RECORD_ID_COLUMN_NAME);
  }

  @Test
  public void makeBatch_fiftyElements() {
    PartialColumnBatchAccumulator accumulator = PartialColumnBatchAccumulator.withConfig(TEST_ENCRYPT_CONFIG);

    PartialColumnBatchAccumulatorTest.addTestRecordsToAccumulator(accumulator, fiftySampleRecords);

    assertThat(accumulator.makeBatch().elementsCount()).isEqualTo(50);
  }

  @Test
  public void makeBatch_elementsContainRecordId() {
    ImmutableList<String> testRecordIds =
      fiftySampleRecords.stream().map(FlatRecord::getRecordId).collect(toImmutableList());
    PartialColumnBatchAccumulator accumulator = PartialColumnBatchAccumulator.withConfig(TEST_ENCRYPT_CONFIG);

    PartialColumnBatchAccumulatorTest.addTestRecordsToAccumulator(accumulator, fiftySampleRecords);

    PartialColumnDlpTable batchTable = accumulator.makeBatch().get();

    int idIndex =
      batchTable
        .getTable()
        .getHeadersList()
        .indexOf(
          FieldId.newBuilder()
            .setName(accumulator.recordIdColumnName())
            .build());

    ImmutableList<String> batchRecordIds =
      batchTable.getTable().getRowsList().stream()
        .map(row -> row.getValues(idIndex).getStringValue()).collect(toImmutableList());

    assertThat(batchRecordIds).containsExactlyElementsIn(testRecordIds).inOrder();
  }

  @Test
  public void makeBatch_containsValidDeidentifyConfig() {
    FlatRecord testContactRecord =
      TestResourceLoader.classPath()
        .forProto(FlatRecord.class)
        .loadJson("flat_records/test_contact_flatrecord.json");

    PartialColumnBatchAccumulator accumulator = PartialColumnBatchAccumulator.withConfig(TEST_ENCRYPT_CONFIG);
    accumulator.addElement(testContactRecord);


    ProtoTruth.assertThat(accumulator.makeBatch().get().getDeidentifyConfig())
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        DeidentifyConfig.newBuilder()
          .setRecordTransformations(
            RecordTransformations.newBuilder()
              .addFieldTransformations(
                FieldTransformation.newBuilder()
                  .addFields(FieldId.newBuilder().setName("$.contacts[0].contact.number").build())
                  .addFields(FieldId.newBuilder().setName("$.contacts[1].contact.number").build())
                  .addFields(FieldId.newBuilder().setName("$.contacts[2].contact.number").build())
                  .setPrimitiveTransformation(CONTACT_TRANSFORM).build())
              .addFieldTransformations(
                FieldTransformation.newBuilder()
                  .addFields(FieldId.newBuilder().setName("$.emails[0]").build())
                  .setPrimitiveTransformation(EMAIL_TRANSFORM)
                  .build()))
          .build());
  }

  /**
   * Returns the first record index that was the accumulator did not add.
   */
  private static int addTestRecordsToAccumulator(PartialColumnBatchAccumulator accumulator, List<FlatRecord> testRecords) {
    int successRecordIndex = 0;
    for (FlatRecord record : testRecords) {
      if (accumulator.addElement(record)) {
        successRecordIndex++;
      }
    }

    return successRecordIndex;
  }
}