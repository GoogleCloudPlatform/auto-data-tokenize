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

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.KeyMaterialType;
import com.google.cloud.solutions.autotokenize.common.DeIdentifiedRecordSchemaConverter;
import com.google.cloud.solutions.autotokenize.common.FlatRecordConvertFn;
import com.google.cloud.solutions.autotokenize.common.RecordFlattener;
import com.google.cloud.solutions.autotokenize.encryptors.DaeadEncryptingValueTokenizer.DaeadEncryptingValueTokenizerFactory;
import com.google.cloud.solutions.autotokenize.testing.FlatRecordsCheckerFn;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class ValueEncryptionTransformTest {

  private static final String TEST_ENCRYPTION_KEYSET_JSON =
      TestResourceLoader.classPath().loadAsString("test_encryption_key.json");
  private final ImmutableSet<String> encryptColumnNames;
  private final Schema inputSchema;
  private final ImmutableList<GenericRecord> testRecords;
  private final ImmutableList<FlatRecord> expectedRecords;
  @Rule public TestPipeline p = TestPipeline.create();

  public ValueEncryptionTransformTest(
      String testConditionName,
      ImmutableSet<String> encryptColumnNames,
      String inputSchemaFile,
      ImmutableList<String> testRecordFiles,
      ImmutableList<String> expectedRecordFiles) {

    this.encryptColumnNames = encryptColumnNames;

    inputSchema = TestResourceLoader.classPath().forAvro().asSchema(inputSchemaFile);

    testRecords =
        TestResourceLoader.classPath()
            .forAvro()
            .withSchemaFile(inputSchemaFile)
            .loadAllRecords(testRecordFiles);

    Schema expectedSchema =
        DeIdentifiedRecordSchemaConverter.withOriginalSchema(inputSchema)
            .withEncryptColumnKeys(encryptColumnNames)
            .updatedSchema();

    RecordFlattener<GenericRecord> recordFlattener = RecordFlattener.forGenericRecord();

    expectedRecords =
        TestResourceLoader.classPath()
            .forAvro()
            .withSchema(expectedSchema)
            .loadAllRecords(expectedRecordFiles)
            .stream()
            .map(recordFlattener::flatten)
            .collect(toImmutableList());
  }

  @Parameters(name = "{0}")
  public static ImmutableList<Object[]> testingParameters() {
    return ImmutableList.<Object[]>builder()
        .add(
            new Object[] {
              "contacts schema valid",
              ImmutableSet.of("$.contact_records.contacts.contact.number"),
              "avro_records/contacts_schema/person_name_union_null_long_contact_schema.json",
              ImmutableList.of(
                  "avro_records/contacts_schema/john_doe_contact_plain_avro_record.json",
                  "avro_records/contacts_schema/jane_doe_contact_plain_avro_record.json"),
              ImmutableList.of(
                  "avro_records/contacts_schema/john_doe_contact_encrypted_avro_record.json",
                  "avro_records/contacts_schema/jane_doe_contact_encrypted_avro_record.json"),
            })
        .add(
            new Object[] {
              "records with namespace schema valid",
              ImmutableSet.of("$.Root.contact.root.Contact.nums.root.contact.Nums.number"),
              "avro_records/contact_records_with_namespace/bq_contacts_schema.json",
              ImmutableList.of(
                  "avro_records/contact_records_with_namespace/white_revolution_contact.json"),
              ImmutableList.of(
                  "avro_records/contact_records_with_namespace/white_revolution_contact_encrypted.json"),
            })
        .add(
            new Object[] {
              "user data with long -> string transform",
              ImmutableSet.of("$.kylosample.cc", "$.kylosample.email", "$.kylosample.ip_address"),
              "avro_records/userdata_records/schema.json",
              ImmutableList.of(
                  "avro_records/userdata_records/record-1.json",
                  "avro_records/userdata_records/record-2.json"),
              ImmutableList.of(
                  "avro_records/userdata_records/encrypted/record-1.json",
                  "avro_records/userdata_records/encrypted/record-2.json"),
            })
        .add(
            new Object[] {
              "null in encrypted field",
              ImmutableSet.of("$.kylosample.cc"),
              "avro_records/userdata_records/schema.json",
              ImmutableList.of("avro_records/userdata_records/record-null-cc.json"),
              ImmutableList.of("avro_records/userdata_records/encrypted/record-null-cc.json"),
            })
        .build();
  }

  @Test
  public void expand_valid() {
    PCollection<FlatRecord> transformedRecords =
        p.apply("load test avro data", Create.of(testRecords).withCoder(AvroCoder.of(inputSchema)))
            .apply(MapElements.via(FlatRecordConvertFn.forGenericRecord()))
            .apply(Keys.create())
            .apply(
                "test_encrypt_transform",
                ValueEncryptionTransform.builder()
                    .valueTokenizerFactory(
                        new DaeadEncryptingValueTokenizerFactory(
                            TEST_ENCRYPTION_KEYSET_JSON, KeyMaterialType.TINK_GCP_KEYSET_JSON))
                    .encryptColumnNames(encryptColumnNames)
                    .build());

    PAssert.that(transformedRecords)
        .satisfies(
            FlatRecordsCheckerFn.withExpectedRecords(expectedRecords).withoutFlatKeySchema());

    p.run();
  }
}
