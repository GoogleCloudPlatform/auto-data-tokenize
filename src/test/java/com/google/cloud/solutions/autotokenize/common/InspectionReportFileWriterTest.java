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

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.InfoTypeInformation;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.InspectionReport;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.testing.JsonSubject;
import com.google.cloud.solutions.autotokenize.testing.RandomGenericRecordGenerator;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.extensions.proto.ProtoTruth;
import java.io.IOException;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class InspectionReportFileWriterTest {

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private String testOutputFolder;

  private final String expectedSchema;
  private final ImmutableList<ColumnInformation> expectedColumnInformation;

  public InspectionReportFileWriterTest(
      String testConditionName,
      String expectedSchema,
      ImmutableList<ColumnInformation> expectedColumnInformation) {
    this.expectedSchema = expectedSchema;
    this.expectedColumnInformation = expectedColumnInformation;
  }

  @Before
  public void createOutputFolder() throws IOException {
    testOutputFolder = temporaryFolder.newFolder().getAbsolutePath();
  }

  @Test
  public void expand_schemaAndSensitiveColumns_valid() {
    var inspectionReport =
        InspectionReport.newBuilder()
            .setAvroSchema(expectedSchema)
            .addAllColumnReport(expectedColumnInformation)
            .setInputPattern("gs://bucket/files*")
            .setSourceType(SourceType.AVRO)
            .build();

    testPipeline
        .apply(Create.of(inspectionReport).withCoder(ProtoCoder.of(InspectionReport.class)))
        .apply(InspectionReportFileWriter.create(testOutputFolder));

    testPipeline.run().waitUntilFinish();

    JsonSubject.assertThat(
            TestResourceLoader.absolutePath().loadAsString(testOutputFolder + "/schema.json"))
        .isEqualTo(expectedSchema);
    ProtoTruth.assertThat(
            TestResourceLoader.absolutePath()
                .forProto(ColumnInformation.class)
                .loadAllJsonFilesLike(testOutputFolder, "col-*"))
        .containsExactlyElementsIn(expectedColumnInformation);
  }

  @Parameters(name = "{0}")
  public static ImmutableList<Object[]> testParameters() {
    return ImmutableList.of(
        new Object[] {
          "two columns",
          RandomGenericRecordGenerator.SCHEMA_STRING,
          ImmutableList.of(
              ColumnInformation.newBuilder()
                  .setColumnName("$.testrecord.name")
                  .addInfoTypes(
                      InfoTypeInformation.newBuilder().setCount(1000).setInfoType("PERSON_NAME"))
                  .build())
        },
        new Object[] {
          "no columns", RandomGenericRecordGenerator.SCHEMA_STRING, ImmutableList.of()
        });
  }
}
