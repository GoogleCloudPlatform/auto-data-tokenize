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

import static com.google.cloud.solutions.autotokenize.common.CsvRowFlatRecordConvertors.csvRowToFlatRecordAndSchemaFn;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.JdbcConfiguration;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.JdbcConfiguration.PasswordsCase;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.common.CsvIO.CsvParse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Sample;
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
 * PCollection<FileIO.ReadableFile> readableFiles = ...;
 *
 * TupleTag<AutoDlpMessages.FlatRecord> recordsTag = new TupleTag<>();
 * TupleTag<String> avroSchemaTag = new TupleTag<>();
 *
 * PCollectionTuple recordsSchemaTuple =
 *   readableFile
 *     .apply("ReadFlatRecord",
 *       TransformingFileReader
 *         .forSourceType("AVRO")
 *         .withInputPattern("gs://...")
 *         .withRecordsTag(recordsTag)
 *         .withAvroSchemaTag(avroSchemaTag));
 *
 * PCollection<AutoDlpMessages.FlatRecord> flatRecords = recordsSchemaTuple.get(recordsTag);
 *
 * }</pre>
 */
@AutoValue
public abstract class TransformingReader extends PTransform<PBegin, PCollectionTuple> {

  abstract @Nullable String inputPattern();

  abstract SourceType sourceType();

  abstract @Nullable JdbcConfiguration jdbcConfiguration();

  abstract @Nullable SecretsClient secretsClient();

  abstract @Nullable TupleTag<FlatRecord> recordsTag();

  abstract @Nullable TupleTag<String> avroSchemaTag();

  abstract @Nullable BigQueryServices bigQueryServices();

  abstract @Nullable ImmutableList<String> csvHeaders();

  abstract boolean csvFirstRowHeader();

  abstract Builder toBuilder();

  static Builder builder() {
    return new AutoValue_TransformingReader.Builder().csvFirstRowHeader(false);
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder inputPattern(String inputPattern);

    abstract Builder sourceType(SourceType fileType);

    abstract Builder jdbcConfiguration(JdbcConfiguration jdbcConfiguration);

    public abstract Builder secretsClient(SecretsClient value);

    abstract Builder recordsTag(TupleTag<FlatRecord> recordsTag);

    abstract Builder avroSchemaTag(TupleTag<String> avroSchemaTag);

    abstract Builder bigQueryServices(BigQueryServices bigQueryServices);

    abstract Builder csvHeaders(ImmutableList<String> csvHeaders);

    abstract Builder csvFirstRowHeader(boolean csvFirstRowHeader);

    abstract TransformingReader build();
  }

  /** Specify the file type of the input file collection. */
  public static TransformingReader forSourceType(SourceType sourceType) {
    return builder().sourceType(sourceType).build();
  }

  /** Specify the input file pattern to read for sampling. */
  public TransformingReader from(String inputFilePattern) {
    return toBuilder().inputPattern(inputFilePattern).build();
  }

  /** Provides the {@link TupleTag} to use for outputting the converted records. */
  public TransformingReader withRecordsTag(TupleTag<FlatRecord> recordsTag) {
    return toBuilder().recordsTag(recordsTag).build();
  }

  /** Provides the {@link TupleTag} to use for outputting the Avro Schema. */
  public TransformingReader withAvroSchemaTag(TupleTag<String> avroSchemaTag) {
    return toBuilder().avroSchemaTag(avroSchemaTag).build();
  }

  /** Provide a JdbcConfiguration for JDBC Connections. */
  public TransformingReader withJdbcConfiguration(JdbcConfiguration jdbcConfiguration) {
    return toBuilder().jdbcConfiguration(jdbcConfiguration).build();
  }

  /** Provide a JdbcConfiguration for JDBC Connections. */
  public TransformingReader withSecretsClient(SecretsClient secretsClient) {
    return toBuilder().secretsClient(secretsClient).build();
  }

  /** Provide a mechanism to test BigQuery based operations by allowing fakeBigQuery Services. */
  @VisibleForTesting
  public TransformingReader withBigQueryTestServices(BigQueryServices bigQueryServices) {
    return toBuilder().bigQueryServices(bigQueryServices).build();
  }

  /** Provide predefined CSV Headers. */
  public TransformingReader withCsvHeaders(List<String> csvHeaders) {
    if (csvHeaders == null) {
      return this;
    }

    return toBuilder().csvHeaders(ImmutableList.copyOf(csvHeaders)).build();
  }

  /** Configure CSV reader to use first row as header. */
  public TransformingReader withCsvFirstRowHeader(boolean csvFirstRowHeader) {
    return toBuilder().csvFirstRowHeader(csvFirstRowHeader).build();
  }

  @Override
  public PCollectionTuple expand(PBegin pBegin) {
    checkNotNull(recordsTag(), "Provide a TupleTag for retrieving FlatRecord");

    PCollection<KV<FlatRecord, String>> recordsWithSchema =
        pBegin
            .apply(
                "Read" + SourceNames.forType(sourceType()).asCamelCase(), readAndConvertTransform())
            .setCoder(KvCoder.of(ProtoCoder.of(FlatRecord.class), StringUtf8Coder.of()));

    PCollection<FlatRecord> flatRecords =
        recordsWithSchema
            .apply("ExtractRecords", Keys.create())
            .setCoder(ProtoCoder.of(FlatRecord.class));

    var recordTuple = PCollectionTuple.of(recordsTag(), flatRecords);

    if (avroSchemaTag() != null) {
      PCollection<String> schema =
          recordsWithSchema
              .apply("ExtractSchema", Values.create())
              .setCoder(StringUtf8Coder.of())
              .apply(Sample.any(1));

      return recordTuple.and(avroSchemaTag(), schema);
    }

    return recordTuple;
  }

  /**
   * Applies {@link #sourceType()} appropriate reader and converts the {@link GenericRecord} into
   * {@link FlatRecord}.
   */
  private PTransform<PBegin, PCollection<KV<FlatRecord, String>>> readAndConvertTransform() {
    checkArgument(isNotEmpty(inputPattern()), "Input pattern must not be empty");

    switch (sourceType()) {
      case AVRO:
        return AvroIO.parseGenericRecords(FlatRecordConvertFn.forGenericRecord())
            .from(inputPattern());

      case PARQUET:
        return ParquetIO.parseGenericRecords(FlatRecordConvertFn.forGenericRecord())
            .from(inputPattern());

      case BIGQUERY_TABLE:
        return bigQueryReader().from(inputPattern());

      case BIGQUERY_QUERY:
        return bigQueryReader()
            .fromQuery(inputPattern())
            .usingStandardSql()
            .withoutResultFlattening();

      case JDBC_TABLE:
        return TransformingJdbcIO.create(jdbcConfiguration(), secretsClient(), inputPattern());

      case JDBC_QUERY:
        return TransformingJdbcIO.create(jdbcConfiguration(), secretsClient(), inputPattern())
            .withQuerySource();

      case CSV_FILE:
        return makeCsvReader();

      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported File type (%s). should be one of %s",
                sourceType(), Arrays.toString(SourceType.values())));
    }
  }

  /** Custom database IO to extract records in the provided table from a JDBC database. */
  @AutoValue
  abstract static class TransformingJdbcIO
      extends PTransform<PBegin, PCollection<KV<FlatRecord, String>>> {

    public static TransformingJdbcIO create(
        JdbcConfiguration jdbcConfiguration, SecretsClient secretsClient, String inputPattern) {
      return new AutoValue_TransformingReader_TransformingJdbcIO(
          jdbcConfiguration, secretsClient, inputPattern, false);
    }

    abstract JdbcConfiguration jdbcConfiguration();

    abstract @Nullable SecretsClient secretsClient();

    abstract String inputPattern();

    abstract boolean querySource();

    TransformingJdbcIO withQuerySource() {
      return new AutoValue_TransformingReader_TransformingJdbcIO(
          jdbcConfiguration(), secretsClient(), inputPattern(), true);
    }

    @Override
    public PCollection<KV<FlatRecord, String>> expand(PBegin pBegin) {
      return pBegin
          .apply(
              JdbcIO.readRows()
                  .withDataSourceConfiguration(
                      DataSourceConfiguration.create(
                              jdbcConfiguration().getDriverClassName(),
                              jdbcConfiguration().getConnectionUrl())
                          .withUsername(jdbcConfiguration().getUserName())
                          .withPassword(extractPassword()))
                  .withQuery(querySource() ? inputPattern() : makeQuery()))
          .apply(MapElements.via(FlatRecordConvertFn.forBeamRow()));
    }

    private String extractPassword() {
      return PasswordsCase.PASSWORD.equals(jdbcConfiguration().getPasswordsCase())
          ? jdbcConfiguration().getPassword()
          : secretsClient().accessSecret(jdbcConfiguration().getPasswordSecretsKey());
    }

    private String makeQuery() {
      var queryBuilder = new StringBuilder().append("SELECT * FROM ").append(inputPattern());

      if (isNotBlank(jdbcConfiguration().getFilterClause())) {
        queryBuilder.append(" WHERE ").append(jdbcConfiguration().getFilterClause());
      }

      return queryBuilder.append(';').toString();
    }
  }

  private BigQueryIO.TypedRead<KV<FlatRecord, String>> bigQueryReader() {
    var reader = BigQueryIO.read(FlatRecordConvertFn.forBigQueryTableRow()).useAvroLogicalTypes();

    if (bigQueryServices() != null) {
      return reader
          .withTestServices(bigQueryServices())
          // Disable Validation only for testing
          .withoutValidation();
    }

    return reader;
  }

  private CsvParse<KV<FlatRecord, String>> makeCsvReader() {
    var csvIo = CsvIO.parse(inputPattern(), csvRowToFlatRecordAndSchemaFn());

    if (csvHeaders() != null && csvHeaders().size() > 0) {
      csvIo = csvIo.withHeaders(csvHeaders());
    }

    if (csvFirstRowHeader()) {
      csvIo = csvIo.withUseFirstRowHeader();
    }

    return csvIo;
  }
}
