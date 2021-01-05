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

package com.google.cloud.solutions.autotokenize.common.io;

import static java.lang.String.format;
import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;


/**
 * <b>Copied from <a href="https://beam.apache.org/releases/javadoc/2.25.0/org/apache/beam/sdk/io/parquet/ParquetIO.html">ParquetIO</a></b>
 *
 * {@link TransformingParquetIO} returns a PCollection for Parquet files. The elements in the {@link
 * PCollection} are POJO objects derived by converting Avro {@link GenericRecord} using the provided
 * convert function through {@link TransformingParquetIO#parseFilesGenericRecords(SerializableFunction)}.
 * This IO has been created to provide parity with {@link AvroIO#parseFilesGenericRecords(SerializableFunction)}
 */
public abstract class TransformingParquetIO {

  public static <T> ReadFiles<T> parseFilesGenericRecords(
      SerializableFunction<GenericRecord, T> parseFn) {
    return new AutoValue_TransformingParquetIO_ReadFiles.Builder<T>()
        .setParseFn(parseFn)
        .setSplittable(false)
        .build();
  }

  /**
   * Copied from the ParquetIO#readFiles with an additional transform using the provided Parse
   * function.
   *
   * @param <T> The type of POJO created by the Parsing function.
   */
  @AutoValue
  public abstract static class ReadFiles<T> extends
      PTransform<PCollection<FileIO.ReadableFile>, PCollection<T>> {

    abstract @Nullable
    Schema getSchema();

    abstract @Nullable
    GenericData getAvroDataModel();

    abstract @Nullable
    Schema getEncoderSchema();

    abstract @Nullable
    Schema getProjectionSchema();

    abstract @Nullable
    Coder<T> getCoder();

    abstract @Nullable
    SerializableFunction<GenericRecord, T> getParseFn();

    abstract boolean isSplittable();

    abstract ReadFiles.Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract ReadFiles.Builder<T> setSchema(Schema schema);

      abstract ReadFiles.Builder<T> setAvroDataModel(GenericData model);

      abstract ReadFiles.Builder<T> setEncoderSchema(Schema schema);

      abstract ReadFiles.Builder<T> setProjectionSchema(Schema schema);

      abstract ReadFiles.Builder<T> setCoder(Coder<T> coder);

      abstract ReadFiles.Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn);

      abstract ReadFiles.Builder<T> setSplittable(boolean split);

      abstract ReadFiles<T> build();
    }

    /**
     * Define the Avro data model; see {@link AvroParquetReader.Builder#withDataModel(GenericData)}.
     */
    public ReadFiles<T> withAvroDataModel(GenericData model) {
      return toBuilder().setAvroDataModel(model).build();
    }

    public ReadFiles<T> withProjection(Schema projectionSchema, Schema encoderSchema) {
      return toBuilder()
          .setProjectionSchema(projectionSchema)
          .setEncoderSchema(encoderSchema)
          .setSplittable(true)
          .build();
    }

    public ReadFiles<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
    }

    /**
     * Enable the Splittable reading.
     */
    public ReadFiles<T> withSplit() {
      return toBuilder().setSplittable(true).build();
    }

    /**
     * convert {@link GenericRecord} into other type using provided parsing Function.
     */
    public ReadFiles<T> withParseFn(SerializableFunction<GenericRecord, T> parseFn) {
      return toBuilder().setParseFn(parseFn).build();
    }

    @Override
    public PCollection<T> expand(PCollection<FileIO.ReadableFile> input) {
      checkState(!(getSchema() == null && getParseFn() == null),
          "provide one of schema and parseFn, both are null.");

      if (isSplittable()) {
        return input
            .apply(ParDo.of(new ReadFiles.SplitReadFn<>(getAvroDataModel(), getProjectionSchema(),
                getParseFn())))
            .setCoder(inferCoder(getCoder(), getParseFn(), input.getPipeline().getCoderRegistry()));
      }
      return input
          .apply(ParDo.of(new ReadFiles.ReadFn<>(getAvroDataModel(), getParseFn())))
          .setCoder(inferCoder(getCoder(), getParseFn(), input.getPipeline().getCoderRegistry()));
    }

    private static <T> Coder<T> inferCoder(
        @Nullable Coder<T> explicitCoder,
        SerializableFunction<GenericRecord, T> parseFn,
        CoderRegistry coderRegistry) {
      if (explicitCoder != null) {
        return explicitCoder;
      }
      // If a coder was not specified explicitly, infer it from parse fn.
      try {
        return coderRegistry.getCoder(TypeDescriptors.outputOf(parseFn));
      } catch (CannotProvideCoderException e) {
        throw new IllegalArgumentException(
            "Unable to infer coder for output of parseFn. Specify it explicitly using withCoder().",
            e);
      }
    }

    @DoFn.BoundedPerElement
    static class SplitReadFn<T> extends DoFn<FileIO.ReadableFile, T> {

      private final Class<? extends GenericData> modelClass;
      private static final GoogleLogger LOG = GoogleLogger.forEnclosingClass();
      private final String requestSchemaString;
      // Default initial splitting the file into blocks of 64MB. Unit of SPLIT_LIMIT is byte.
      private static final long SPLIT_LIMIT = 64000000;

      private final SerializableFunction<GenericRecord, T> parseFn;

      SplitReadFn(GenericData model, Schema requestSchema,
          SerializableFunction<GenericRecord, T> parseFn) {

        this.modelClass = model != null ? model.getClass() : null;
        this.requestSchemaString = requestSchema != null ? requestSchema.toString() : null;
        this.parseFn = checkNotNull(parseFn, "parseFn");
      }

      ParquetFileReader getParquetFileReader(FileIO.ReadableFile file) throws Exception {
        ParquetReadOptions options = HadoopReadOptions.builder(getConfWithModelClass()).build();
        return ParquetFileReader
            .open(new ReadFiles.BeamParquetInputFile(file.openSeekable()), options);
      }

      @ProcessElement
      public void processElement(
          @Element FileIO.ReadableFile file,
          RestrictionTracker<OffsetRange, Long> tracker,
          OutputReceiver<T> outputReceiver)
          throws Exception {
        LOG.atInfo().log(
            "start %s to %s", tracker.currentRestriction().getFrom(),
            tracker.currentRestriction().getTo());
        Configuration conf = getConfWithModelClass();
        GenericData model = null;
        if (modelClass != null) {
          model = (GenericData) modelClass.getMethod("get").invoke(null);
        }
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>(model);
        if (requestSchemaString != null) {
          AvroReadSupport.setRequestedProjection(
              conf, new Schema.Parser().parse(requestSchemaString));
        }
        ParquetReadOptions options = HadoopReadOptions.builder(conf).build();
        ParquetFileReader reader =
            ParquetFileReader
                .open(new ReadFiles.BeamParquetInputFile(file.openSeekable()), options);
        FilterCompat.Filter filter = checkNotNull(options.getRecordFilter(), "filter");
        Configuration hadoopConf = ((HadoopReadOptions) options).getConf();
        FileMetaData parquetFileMetadata = reader.getFooter().getFileMetaData();
        MessageType fileSchema = parquetFileMetadata.getSchema();
        Map<String, String> fileMetadata = parquetFileMetadata.getKeyValueMetaData();
        ReadSupport.ReadContext readContext =
            readSupport.init(
                new InitContext(
                    hadoopConf, Maps.transformValues(fileMetadata, ImmutableSet::of), fileSchema));
        ColumnIOFactory columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy());

        RecordMaterializer<GenericRecord> recordConverter =
            readSupport.prepareForRead(hadoopConf, fileMetadata, fileSchema, readContext);
        reader.setRequestedSchema(readContext.getRequestedSchema());
        MessageColumnIO columnIO =
            columnIOFactory.getColumnIO(readContext.getRequestedSchema(), fileSchema, true);
        long currentBlock = tracker.currentRestriction().getFrom();
        for (int i = 0; i < currentBlock; i++) {
          reader.skipNextRowGroup();
        }
        while (tracker.tryClaim(currentBlock)) {
          PageReadStore pages = reader.readNextRowGroup();
          LOG.atFine()
              .log("block %s read in memory. row count = %s", currentBlock, pages.getRowCount());
          currentBlock += 1;
          RecordReader<GenericRecord> recordReader =
              columnIO.getRecordReader(
                  pages, recordConverter, options.useRecordFilter() ? filter : FilterCompat.NOOP);
          long currentRow = 0;
          long totalRows = pages.getRowCount();
          while (currentRow < totalRows) {
            try {
              GenericRecord record;
              currentRow += 1;
              try {
                record = recordReader.read();
              } catch (RecordMaterializer.RecordMaterializationException e) {
                LOG.atWarning().log(
                    "skipping a corrupt record at %s in block %s in file %s",
                    currentRow,
                    currentBlock,
                    file.toString());
                continue;
              }
              if (record == null) {
                // only happens with FilteredRecordReader at end of block
                LOG.atFine().log(
                    "filtered record reader reached end of block in block %s in file %s",
                    currentBlock,
                    file.toString());
                break;
              }
              if (recordReader.shouldSkipCurrentRecord()) {
                // this record is being filtered via the filter2 package
                LOG.atFine().log(
                    "skipping record at %s in block %s in file %s",
                    currentRow,
                    currentBlock,
                    file.toString());
                continue;
              }
              outputReceiver.output(parseFn.apply(record));
            } catch (RuntimeException e) {

              throw new ParquetDecodingException(
                  format(
                      "Can not read value at %d in block %d in file %s",
                      currentRow, currentBlock, file.toString()),
                  e);
            }
          }
          LOG.atFine().log(
              "Finish processing %s rows from block %s in file %s", currentRow, currentBlock - 1,
              file.toString());
        }
      }

      public Configuration getConfWithModelClass() throws Exception {
        Configuration conf = new Configuration();
        GenericData model = null;
        if (modelClass != null) {
          model = (GenericData) modelClass.getMethod("get").invoke(null);
        }
        conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, model != null
            && (model.getClass() == GenericData.class || model.getClass() == SpecificData.class));
        return conf;
      }

      @GetInitialRestriction
      public OffsetRange getInitialRestriction(@Element FileIO.ReadableFile file) throws Exception {
        ParquetFileReader reader = getParquetFileReader(file);
        return new OffsetRange(0, reader.getRowGroups().size());
      }

      @SplitRestriction
      public void split(
          @Restriction OffsetRange restriction,
          OutputReceiver<OffsetRange> out,
          @Element FileIO.ReadableFile file)
          throws Exception {
        ParquetFileReader reader = getParquetFileReader(file);
        List<BlockMetaData> rowGroups = reader.getRowGroups();
        for (OffsetRange offsetRange :
            splitBlockWithLimit(
                restriction.getFrom(), restriction.getTo(), rowGroups, SPLIT_LIMIT)) {
          out.output(offsetRange);
        }
      }

      public ArrayList<OffsetRange> splitBlockWithLimit(
          long start, long end, List<BlockMetaData> blockList, long limit) {
        ArrayList<OffsetRange> offsetList = new ArrayList<>();
        long totalSize = 0;
        long rangeStart = start;
        for (long rangeEnd = start; rangeEnd < end; rangeEnd++) {
          totalSize += blockList.get((int) rangeEnd).getTotalByteSize();
          if (totalSize >= limit) {
            offsetList.add(new OffsetRange(rangeStart, rangeEnd + 1));
            rangeStart = rangeEnd + 1;
            totalSize = 0;
          }
        }
        if (totalSize != 0) {
          offsetList.add(new OffsetRange(rangeStart, end));
        }
        return offsetList;
      }

      @NewTracker
      public RestrictionTracker<OffsetRange, Long> newTracker(
          @Restriction OffsetRange restriction, @Element FileIO.ReadableFile file)
          throws Exception {
        ReadFiles.SplitReadFn.CountAndSize recordCountAndSize = getRecordCountAndSize(file,
            restriction);
        return new ReadFiles.BlockTracker(
            restriction,
            Math.round(recordCountAndSize.getSize()),
            Math.round(recordCountAndSize.getCount()));
      }

      @GetRestrictionCoder
      public OffsetRange.Coder getRestrictionCoder() {
        return new OffsetRange.Coder();
      }

      @GetSize
      public double getSize(@Element FileIO.ReadableFile file, @Restriction OffsetRange restriction)
          throws Exception {
        return getRecordCountAndSize(file, restriction).getSize();
      }

      private ReadFiles.SplitReadFn.CountAndSize getRecordCountAndSize(FileIO.ReadableFile file,
          OffsetRange restriction)
          throws Exception {
        ParquetFileReader reader = getParquetFileReader(file);
        double size = 0;
        double recordCount = 0;
        for (long i = restriction.getFrom(); i < restriction.getTo(); i++) {
          BlockMetaData block = reader.getRowGroups().get((int) i);
          recordCount += block.getRowCount();
          size += block.getTotalByteSize();
        }
        return CountAndSize.create(recordCount, size);
      }

      @AutoValue
      abstract static class CountAndSize {

        public static CountAndSize create(double newCount, double newSize) {
          return new AutoValue_TransformingParquetIO_ReadFiles_SplitReadFn_CountAndSize(newCount,
              newSize);
        }

        abstract double getCount();

        abstract double getSize();
      }
    }

    public static class BlockTracker extends OffsetRangeTracker {

      private long totalWork;
      private long progress;
      private long approximateRecordSize;

      public BlockTracker(OffsetRange range, long totalByteSize, long recordCount) {
        super(range);
        if (recordCount != 0) {
          this.approximateRecordSize = totalByteSize / recordCount;
          // Ensure that totalWork = approximateRecordSize * recordCount
          this.totalWork = approximateRecordSize * recordCount;
          this.progress = 0;
        }
      }

      public void makeProgress() throws Exception {
        progress += approximateRecordSize;
        if (progress > totalWork) {
          throw new IOException("Making progress out of range");
        }
      }

      @Override
      // TODO(BEAM-10842): Refine the BlockTracker to provide better progress.
      public Progress getProgress() {
        return super.getProgress();
      }
    }

    static class ReadFn<T> extends DoFn<FileIO.ReadableFile, T> {

      private final Class<? extends GenericData> modelClass;

      private final SerializableFunction<GenericRecord, T> parseFn;

      ReadFn(GenericData model, SerializableFunction<GenericRecord, T> parseFn) {
        this.modelClass = model != null ? model.getClass() : null;
        this.parseFn = checkNotNull(parseFn, "parseFn");
      }

      @ProcessElement
      public void processElement(ProcessContext processContext) throws Exception {
        FileIO.ReadableFile file = processContext.element();

        if (!file.getMetadata().isReadSeekEfficient()) {
          ResourceId filename = file.getMetadata().resourceId();
          throw new RuntimeException(String.format("File has to be seekable: %s", filename));
        }

        SeekableByteChannel seekableByteChannel = file.openSeekable();

        AvroParquetReader.Builder<GenericRecord> builder =
            AvroParquetReader.builder(new ReadFiles.BeamParquetInputFile(seekableByteChannel));

        Configuration conf = new Configuration();

        if (modelClass != null) {
          // all GenericData implementations have a static get method
          builder = builder.withDataModel((GenericData) modelClass.getMethod("get").invoke(null));
        }

        try (ParquetReader<GenericRecord> reader = builder.withConf(conf).build()) {
          GenericRecord read;
          while ((read = reader.read()) != null) {
            processContext.output(parseFn.apply(read));
          }
        }
      }
    }

    private static class BeamParquetInputFile implements InputFile {

      private final SeekableByteChannel seekableByteChannel;

      BeamParquetInputFile(SeekableByteChannel seekableByteChannel) {
        this.seekableByteChannel = seekableByteChannel;
      }

      @Override
      public long getLength() throws IOException {
        return seekableByteChannel.size();
      }

      @Override
      public SeekableInputStream newStream() {
        return new DelegatingSeekableInputStream(Channels.newInputStream(seekableByteChannel)) {

          @Override
          public long getPos() throws IOException {
            return seekableByteChannel.position();
          }

          @Override
          public void seek(long newPos) throws IOException {
            seekableByteChannel.position(newPos);
          }
        };
      }
    }
  }
}
