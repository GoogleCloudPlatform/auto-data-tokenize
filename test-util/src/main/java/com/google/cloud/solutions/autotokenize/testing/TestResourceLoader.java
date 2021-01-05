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

package com.google.cloud.solutions.autotokenize.testing;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.cloud.solutions.autotokenize.common.util.JsonConvertor;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Supports loading testing files and decoding into formats like String, Proto and AVRO for ease in
 * writing unit tests.
 */
public abstract class TestResourceLoader {

  private static final String TEST_RESOURCE_FOLDER = "test";

  public static ResourceActions local() {
    return new ResourceActions(new LocalTestResourceLoader());
  }

  public static ResourceActions classPath() {
    return new ResourceActions(new ClassPathTestResourceLoader());
  }

  public static ResourceActions classPathWithContext(Class<?> contextClass) {
    return new ResourceActions(new ClassPathTestResourceLoader(contextClass));
  }

  public abstract String loadResourceAsString(String resourcePath) throws IOException;

  public final String loadAsString(String resourcePath) {
    try {
      return loadResourceAsString(resourcePath);
    } catch (IOException ioException) {
      throw new ResourceLoadException(resourcePath, ioException);
    }
  }

  public static final class LocalTestResourceLoader extends TestResourceLoader {

    private LocalTestResourceLoader() {
    }

    @Override
    public String loadResourceAsString(String resourceFileName) throws IOException {

      byte[] bytes =
          Files.readAllBytes(Paths.get("src", TEST_RESOURCE_FOLDER, "resources", resourceFileName));
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  public static final class ClassPathTestResourceLoader extends TestResourceLoader {

    private final Class<?> contextClass;

    private ClassPathTestResourceLoader(Class<?> contextClass) {
      this.contextClass = contextClass;
    }

    private ClassPathTestResourceLoader() {
      this(null);
    }

    @Override
    public String loadResourceAsString(String resourcePath) throws IOException {

      try (BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(
                  loadResource(resourcePath).openStream(), StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining("\n"));
      }
    }

    @SuppressWarnings("UnstableApiUsage")
    private URL loadResource(String resourcePath) {
      return (contextClass == null)
          ? Resources.getResource(resourcePath)
          : Resources.getResource(contextClass, resourcePath);
    }
  }

  public static class ResourceLoadException extends RuntimeException {

    public ResourceLoadException(String fileName, Throwable cause) {
      super("Error reading test resource: " + fileName, cause);
    }
  }

  public static class ResourceActions {

    private final TestResourceLoader resourceLoader;

    private ResourceActions(TestResourceLoader resourceLoader) {
      this.resourceLoader = resourceLoader;
    }

    public String loadAsString(String resourceUri) {
      return resourceLoader.loadAsString(resourceUri);
    }

    public <T extends Message> ProtoActions<T> forProto(Class<T> protoClazz) {
      return new ProtoActions<T>() {
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
        public ImmutableList<T> loadAllJsonFiles(List<String> jsonPbFiles) {
          return jsonPbFiles.stream().map(this::loadJson).collect(toImmutableList());
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

      ImmutableList<P> loadAllJsonFiles(List<String> jsonPbFiles);
    }

    public class AvroActionsBuilder {

      public Schema asSchema(String resourcePath) {
        return new Schema.Parser().parse(resourceLoader.loadAsString(resourcePath));
      }

      public AvroRecordActions withSchema(Schema schema) {
        return new AvroRecordActions(schema);
      }

      public AvroRecordActions withSchemaFile(String schemaFile) {
        return new AvroRecordActions(asSchema(schemaFile));
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
}
