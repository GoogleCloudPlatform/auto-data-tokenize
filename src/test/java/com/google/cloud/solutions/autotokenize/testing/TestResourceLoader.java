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

package com.google.cloud.solutions.autotokenize.testing;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.cloud.solutions.autotokenize.common.util.JsonConvertor;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

/**
 * Supports loading testing files and decoding into formats like String, Proto and AVRO for ease in
 * writing unit tests.
 */
public interface TestResourceLoader {

  URL loadResource(String resourcePath) throws MalformedURLException;

  default String loadResourceAsString(String resourcePath) throws IOException {

    try (var reader =
        new BufferedReader(
            new InputStreamReader(
                loadResource(resourcePath).openStream(), StandardCharsets.UTF_8))) {
      return reader.lines().collect(Collectors.joining("\n"));
    }
  }

  default InputStream loadResourceInputStream(String resourcePath) throws IOException {
    return loadResource(resourcePath).openStream();
  }

  default String loadAsString(String resourcePath) {
    try {
      return loadResourceAsString(resourcePath);
    } catch (IOException ioException) {
      throw new ResourceLoadException(resourcePath, ioException);
    }
  }

  static ResourceActions absolutePath() {
    return new ResourceActions(resourcePath -> new File(resourcePath).toURI().toURL());
  }

  @SuppressWarnings("UnstableApiUsage")
  static ResourceActions classPath() {
    return new ResourceActions(Resources::getResource);
  }

  final class ResourceActions {

    private final TestResourceLoader resourceLoader;

    private ResourceActions(TestResourceLoader resourceLoader) {
      this.resourceLoader = resourceLoader;
    }

    public String loadAsString(String resourceUri) {
      return resourceLoader.loadAsString(resourceUri);
    }

    public <T> T loadAsJson(String resourceUri, Type jsonClass) {
      return new Gson().fromJson(loadAsString(resourceUri), jsonClass);
    }

    public CopyActions copyTo(File folder) {
      return resourcePath -> {
        var outputFile = File.createTempFile("temp_", "", folder);

        long copiedBytes =
            Files.asByteSink(outputFile)
                .writeFrom(resourceLoader.loadResourceInputStream(resourcePath));

        GoogleLogger.forEnclosingClass()
            .atInfo()
            .log(
                "Copied %s bytes from %s to %s",
                copiedBytes, resourcePath, outputFile.getAbsolutePath());

        return outputFile;
      };
    }

    public <T extends Message> ProtoActions<T> forProto(Class<T> protoClazz) {
      return new ProtoActions<>() {
        @Override
        public T loadJson(String jsonProtoFile) {
          return JsonConvertor.parseJson(resourceLoader.loadAsString(jsonProtoFile), protoClazz);
        }

        @Override
        public T loadText(String textProtoFile) {
          try {
            return TextFormat.parse(resourceLoader.loadAsString(textProtoFile), protoClazz);
          } catch (ParseException parseException) {
            return null;
          }
        }

        @Override
        public ImmutableList<T> loadAllTextFiles(List<String> textPbFiles) {
          return textPbFiles.stream().map(this::loadText).collect(toImmutableList());
        }

        @Override
        public ImmutableList<T> loadAllTextFiles(String... textPbFiles) {
          return loadAllTextFiles(ImmutableList.copyOf(textPbFiles));
        }

        @Override
        public ImmutableList<T> loadAllJsonFiles(List<String> jsonPbFiles) {
          return jsonPbFiles.stream().map(this::loadJson).collect(toImmutableList());
        }

        @Override
        public ImmutableList<T> loadAllJsonFiles(String... jsonPbFiles) {
          return loadAllJsonFiles(ImmutableList.copyOf(jsonPbFiles));
        }
      };
    }

    public AvroActionsBuilder forAvro() {
      return new AvroActionsBuilder();
    }

    public interface ProtoActions<P extends Message> {

      P loadJson(String jsonProtoFile);

      P loadText(String textProtoFile);

      ImmutableList<P> loadAllTextFiles(List<String> textPbFiles);

      ImmutableList<P> loadAllTextFiles(String... textPbFiles);

      ImmutableList<P> loadAllJsonFiles(List<String> jsonPbFiles);

      ImmutableList<P> loadAllJsonFiles(String... jsonPbFiles);
    }

    public interface CopyActions {
      File createFileTestCopy(String resourcePath) throws IOException;
    }

    public class AvroActionsBuilder {

      public Schema asSchema(String resourcePath) {
        return new Schema.Parser().parse(resourceLoader.loadAsString(resourcePath));
      }

      public AvroFileActions readFile(String resourcePath) {
        return new AvroFileActions(resourcePath);
      }

      public AvroRecordActions withSchema(Schema schema) {
        return new AvroRecordActions(schema);
      }

      public AvroRecordActions withSchemaFile(String schemaFile) {
        return new AvroRecordActions(asSchema(schemaFile));
      }
    }

    public class AvroFileActions {

      private final String filePath;

      public AvroFileActions(String filePath) {
        this.filePath = filePath;
      }

      public ImmutableList<GenericRecord> loadAllRecords() {

        try (DataFileStream<GenericRecord> fileStream =
            new DataFileStream<>(
                resourceLoader.loadResourceInputStream(filePath), new GenericDatumReader<>())) {

          ImmutableList.Builder<GenericRecord> recordBuilder = ImmutableList.builder();

          while (fileStream.hasNext()) {
            recordBuilder.add(fileStream.next());
          }

          return recordBuilder.build();
        } catch (Exception e) {
          throw new ResourceLoadException(filePath, e);
        }
      }
    }

    public class AvroRecordActions {

      private final Schema schema;

      public AvroRecordActions(Schema schema) {
        this.schema = schema;
      }

      public GenericRecord loadRecord(String recordResourcePath) {
        return JsonConvertor.convertJsonToAvro(
            schema, resourceLoader.loadAsString(recordResourcePath));
      }

      public ImmutableList<GenericRecord> loadAllRecords(List<String> recordFiles) {
        return ImmutableList.copyOf(recordFiles).stream()
            .map(this::loadRecord)
            .collect(toImmutableList());
      }

      public ImmutableList<GenericRecord> loadAllRecords(String... recordFiles) {
        return loadAllRecords(Arrays.asList(recordFiles));
      }
    }
  }

  final class ResourceLoadException extends RuntimeException {
    public ResourceLoadException(String fileName, Throwable cause) {
      super("Error reading test resource: " + fileName, cause);
    }
  }
}
