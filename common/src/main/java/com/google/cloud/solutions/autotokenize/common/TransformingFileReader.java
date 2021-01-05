/*
 * Copyright 2020 Google LLC
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.common.io.TransformingParquetIO;
import com.google.common.flogger.GoogleLogger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Reads an unknown schema AVRO or Parquet file and outputs a {@code PCollection} derived by
 * transforming {@link GenericRecord} as {@link FlatRecord}. The second output is the {@link Schema}
 * as JSON string.
 *
 * <pre>{@code
 *   PCollection<FileIO.ReadableFile> readableFiles = ...;
 *
 *   TupleTag<AutoDlpMessages.FlatRecord> recordsTag = new TupleTag<>();
 *   TupleTag<String> avroSchemaTag = new TupleTag<>();
 *
 *   PCollectionTuple recordsSchemaTuple =
 *     readableFile
 *       .apply("ReadFlatRecord",
 *         TransformingFileReader
 *           .forFileType("AVRO")
 *           .withInputFilePattern("gs://...")
 *           .withRecordsTag(recordsTag)
 *           .withAvroSchemaTag(avroSchemaTag));
 *
 *   PCollection<AutoDlpMessages.FlatRecord> flatRecords = recordsSchemaTuple.get(recordsTag);
 *
 * }</pre>
 */
@AutoValue
public abstract class TransformingFileReader extends PTransform<PBegin, PCollectionTuple> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  abstract @Nullable String inputFilePattern();

  abstract @Nullable String bigqueryTable();

  abstract @Nullable String bigqueryQuery();

  abstract String fileType();

  abstract @Nullable TupleTag<FlatRecord> recordsTag();

  abstract @Nullable TupleTag<String> avroSchemaTag();

  abstract Builder toBuilder();

  static Builder builder() {
    return new AutoValue_TransformingFileReader.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder inputFilePattern(String inputPattern);

    abstract Builder bigqueryTable(String bigqueryTable);

    abstract Builder bigqueryQuery(String bigqueryQuery);

    abstract Builder fileType(String fileType);

    abstract Builder recordsTag(TupleTag<FlatRecord> recordsTag);

    abstract Builder avroSchemaTag(TupleTag<String> avroSchemaTag);

    abstract TransformingFileReader build();
  }

  /**
   * Specify the file type of the input file collection.
   */
  public static TransformingFileReader forFileType(String fileType) {
    return builder().fileType(fileType).build();
  }

  /**
   * Specify the input file pattern to read for sampling.
   */
  public TransformingFileReader from(String inputFilePattern) {
    return toBuilder().inputFilePattern(inputFilePattern).build();
  }

  /**
   * Provides the {@link TupleTag} to use for outputting the converted records.
   */
  public TransformingFileReader withRecordsTag(TupleTag<FlatRecord> recordsTag) {
    return toBuilder().recordsTag(recordsTag).build();
  }

  /**
   * Provides the {@link TupleTag} to use for outputting the Avro Schema.
   */
  public TransformingFileReader withAvroSchemaTag(TupleTag<String> avroSchemaTag) {
    return toBuilder().avroSchemaTag(avroSchemaTag).build();
  }

  @Override
  public PCollectionTuple expand(PBegin pBegin) {
    checkNotNull(recordsTag(), "Provide a TupleTag for retrieving FlatRecord");

    PCollection<KV<FlatRecord, String>> recordsWithSchema =
      pBegin
        .apply("Read" + fileType(), readAndConvertTransform())
        .setCoder(KvCoder.of(ProtoCoder.of(FlatRecord.class), StringUtf8Coder.of()));

    PCollection<FlatRecord> flatRecords =
      recordsWithSchema.apply("ExtractRecords", Keys.create())
        .setCoder(ProtoCoder.of(FlatRecord.class));

    PCollectionTuple recordTuple = PCollectionTuple.of(recordsTag(), flatRecords);

    if (avroSchemaTag() != null) {
      PCollection<String> schema =
        recordsWithSchema.apply("ExtractSchema", Values.create())
          .setCoder(StringUtf8Coder.of())
          .apply(Distinct.create());

      return recordTuple.and(avroSchemaTag(), schema);
    }

    return recordTuple;
  }

  /**
   * Applies {@link #fileType()} appropriate reader and converts the {@link GenericRecord} into
   * {@link FlatRecord}.
   */
  private PTransform<PBegin, PCollection<KV<FlatRecord, String>>> readAndConvertTransform() {
    checkArgument(
      ((fileType().equals("AVRO") || fileType().equals("PARQUERT")) && inputFilePattern() != null)
        || (fileType().equals("BIGQUERY") && (isNotEmpty(bigqueryQuery()) || isNotEmpty(bigqueryTable()))));

    switch (fileType()) {
      case "AVRO":
        return AvroIO.parseGenericRecords(FlatRecordConvertFn.forGenericRecord()).from(inputFilePattern());
      case "PARQUET":
        return TransformingParquetIO.parseGenericRecords(FlatRecordConvertFn.forGenericRecord()).from(inputFilePattern());

      case "BIGQUERY":
        BigQueryIO.TypedRead<KV<FlatRecord, String>> bqRead = BigQueryIO.read(FlatRecordConvertFn.forBigQueryTableRow()).useAvroLogicalTypes();

        if (bigqueryQuery() != null) {
          return bqRead.fromQuery(bigqueryQuery());
        }
        return bqRead.from(bigqueryTable());
      default:
        throw new IllegalArgumentException(
          String.format("Unsupported File type (%s). should be \"AVRO\" or \"PARQUET\"",
            fileType()));
    }
  }
}
