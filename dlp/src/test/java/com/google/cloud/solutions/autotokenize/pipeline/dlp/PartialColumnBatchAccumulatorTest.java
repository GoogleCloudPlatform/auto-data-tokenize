package com.google.cloud.solutions.autotokenize.pipeline.dlp;


import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.util.Maps;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.common.collect.ImmutableList;
import com.google.privacy.dlp.v2.Value;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class PartialColumnBatchAccumulatorTest {

  private static final DlpEncryptConfig TEST_ENCRYPT_CONFIG =
    DlpEncryptConfig.newBuilder()
      .addTransforms(AutoTokenizeMessages.ColumnTransform.newBuilder().setColumnId("$.contacts.contact.number").build())
      .addTransforms(AutoTokenizeMessages.ColumnTransform.newBuilder().setColumnId("$.emails").build())
      .build();

  @Test
  public void addElement_empty_true() {
    ImmutableList<FlatRecord> contactRecords = new SampleGenerator().buildContactRecords(1);
    PartialColumnBatchAccumulator accumulator = PartialColumnBatchAccumulator.withConfig(TEST_ENCRYPT_CONFIG);

    assertThat(accumulator.addElement(contactRecords.get(0))).isTrue();
  }

  @Test
  public void addElement_noStorage_false() {
    ImmutableList<FlatRecord> contactRecords = new SampleGenerator().buildContactRecords(1);
    PartialColumnBatchAccumulator accumulator = new PartialColumnBatchAccumulator(1, 50, TEST_ENCRYPT_CONFIG);

    assertThat(accumulator.addElement(contactRecords.get(0))).isFalse();
  }

  @Test
  public void addElement_noCells_false() {
    ImmutableList<FlatRecord> contactRecords = new SampleGenerator().buildContactRecords(1);
    PartialColumnBatchAccumulator accumulator = new PartialColumnBatchAccumulator(100, 1, TEST_ENCRYPT_CONFIG);

    assertThat(accumulator.addElement(contactRecords.get(0))).isFalse();
  }

  @Test
  public void addElement_emptyRecordId_throwsException() {
    ImmutableList<FlatRecord> testRecords = new SampleGenerator(true).buildContactRecords(1);
    PartialColumnBatchAccumulator accumulator = PartialColumnBatchAccumulator.withConfig(TEST_ENCRYPT_CONFIG);

    IllegalArgumentException iaex =
      assertThrows(IllegalArgumentException.class, () -> accumulator.addElement(testRecords.get(0)));

    assertThat(iaex).hasMessageThat().contains("Provide FlatRecord with unique RecordId, found empty");
  }

  @Test
  public void addElement_doesNotAddAfterFalse() {
    ImmutableList<FlatRecord> testRecords = new SampleGenerator().buildContactRecords(2000);
    PartialColumnBatchAccumulator accumulator = new PartialColumnBatchAccumulator(50000, 1000, TEST_ENCRYPT_CONFIG);

    int falseRecordCount = 0;
    for (int i = 0; i < testRecords.size(); i++) {
      if (!accumulator.addElement(testRecords.get(i)) && falseRecordCount == 0) {
        falseRecordCount = i;
      }
    }

    assertThat(accumulator.makeBatch().elementsCount()).isEqualTo(falseRecordCount);
  }

  @Test
  public void makeBatch_fiftyElements() {
    ImmutableList<FlatRecord> testRecords = new SampleGenerator().buildContactRecords(50);
    PartialColumnBatchAccumulator accumulator = PartialColumnBatchAccumulator.withConfig(TEST_ENCRYPT_CONFIG);

    for (FlatRecord record : testRecords) {
      accumulator.addElement(record);
    }

    assertThat(accumulator.makeBatch().elementsCount()).isEqualTo(50);
  }

  public static class SampleGenerator {

    private final boolean omitId;
    private final Random random;

    public SampleGenerator(boolean omitId) {
      this.omitId = omitId;
      this.random = new Random();
    }

    public SampleGenerator() {
      this(false);
    }

    private ImmutableList<FlatRecord> buildContactRecords(int count) {

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


        recordListBuilder.add(flatRecordBuilder().putAllValues(valuesMap).putAllFlatKeySchema(flatKeyMap).build());
      }

      return recordListBuilder.build();
    }

    private FlatRecord.Builder flatRecordBuilder() {
      FlatRecord.Builder builder = FlatRecord.newBuilder();
      return (omitId) ? builder : builder.setRecordId(UUID.randomUUID().toString());
    }

    private String randomName() {
      StringBuilder stringBuilder = new StringBuilder();

      random.ints(random.nextInt(26), 'A', 'Z')
        .forEach(i -> stringBuilder.append((char) i));

      return stringBuilder.toString();
    }

    private String randomPhoneNumber(int numLength) {
      StringBuilder builder = new StringBuilder();

      random.ints(numLength, '0', '9')
        .forEach(i -> builder.append((char) i));

      return builder.toString();
    }

  }
}