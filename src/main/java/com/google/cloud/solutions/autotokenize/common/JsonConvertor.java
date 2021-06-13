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

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.jayway.jsonpath.JsonPath;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/** Utility methods to transform to/from JSON for Java-beans, Proto and AVRO types. */
public final class JsonConvertor {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private JsonConvertor() {}

  /**
   * Returns the Object serialized as JSON using Jackson ObjectWriter or Protobuf JsonFormat if the
   * object is a Message.
   *
   * @param object the object to get JSON representation of.
   * @return JSON representation of the object or EMPTY string in case of error.
   */
  public static <T> String asJsonString(T object) {
    try {

      if (object == null) {
        return "";
      }

      if (object instanceof MessageOrBuilder) {
        return convertProtobufMessage((MessageOrBuilder) object);
      }

      if (object instanceof Collection) {
        return "["
            + ((Collection<?>) object)
                .stream().map(JsonConvertor::asJsonString).collect(Collectors.joining(","))
            + "]";
      }

      if (object instanceof Map) {
        return mapAsJsonString((Map<?, ?>) object);
      }

      return convertJavaBeans(object);

    } catch (IOException exp) {
      logger.atSevere().withCause(exp).log("Error in converting to Json");
    }

    return "";
  }

  /** Returns a JSON string of the given Map. */
  private static String mapAsJsonString(Map<?, ?> map) {
    if (map == null || map.isEmpty()) {
      return "{}";
    }

    return "{"
        + map.entrySet().stream()
            .map(
                entry ->
                    String.format("\"%s\": %s", entry.getKey(), asJsonString(entry.getValue())))
            .collect(Collectors.joining(","))
        + "}";
  }

  /** Returns a JSON String representation using Jackson ObjectWriter. */
  private static <T> String convertJavaBeans(T object) throws JsonProcessingException {
    return new ObjectMapper().writer().writeValueAsString(object);
  }

  /** Returns a JSON String representation using Protobuf JsonFormat. */
  public static String convertProtobufMessage(MessageOrBuilder message)
      throws InvalidProtocolBufferException {
    return JsonFormat.printer().print(message);
  }

  /**
   * Returns a proto message by parsing json for the given proto class.
   *
   * @param json the Json representation of the proto message.
   * @param protoClass the proto class to deserialize
   * @param <T> the type of Proto class.
   */
  @SuppressWarnings("unchecked") // Use of generics for creation of Proto message from JSON.
  public static <T extends Message> T parseJson(String json, Class<T> protoClass) {
    try {
      var builder = (Message.Builder) protoClass.getMethod("newBuilder").invoke(null);

      JsonFormat.parser().merge(json, builder);
      return (T) builder.build();
    } catch (Exception exception) {
      logger.atSevere().withCause(exception).atMostEvery(1, TimeUnit.MINUTES).log(
          "error converting json:\n%s", json);
      throw new JsonConversionException("error converting\n" + json, exception);
    }
  }

  /**
   * Returns a list of Proto objects by parsing input collection of protoJson strings.
   *
   * @param jsons the Protobuf objects in json format.
   * @param protoClass the proto class to use for deserialize
   * @param <T> the type of Proto message
   */
  public static <T extends Message> ImmutableList<T> parseAsList(
      Collection<String> jsons, Class<T> protoClass) {
    return jsons.stream()
        .map(json -> JsonConvertor.parseJson(json, protoClass))
        .filter(Objects::nonNull)
        .collect(toImmutableList());
  }

  /**
   * Returns a list of proto objects by parsing the input string as a JSON array.
   *
   * @param jsonArray the JSON array string of proto objects
   * @param protoClass the proto class to use for deserialize
   * @param <T> the type of Proto message
   */
  public static <T extends Message> ImmutableList<T> parseAsList(
      String jsonArray, Class<T> protoClass) {
    return parseAsList(
        JsonPath.parse(jsonArray).<List<LinkedHashMap<String, Object>>>read("$").stream()
            .map(JsonConvertor::asJsonString)
            .collect(toImmutableList()),
        protoClass);
  }

  /**
   * Returns an AVRO record by deserializing the Json representation of the AVRO record.
   *
   * @param schemaJson the AVRO schema in JSON format.
   * @param avroJson the AVRO record in JSON format.
   */
  public static GenericRecord convertJsonToAvro(String schemaJson, String avroJson) {
    return convertJsonToAvro(new Schema.Parser().parse(schemaJson), avroJson);
  }

  /**
   * Returns an AVRO record by deserializing the Json representation of the AVRO record.
   *
   * @param schema the AVRO schema
   * @param avroJson the AVRO record in JSON format.
   */
  public static GenericRecord convertJsonToAvro(Schema schema, String avroJson) {
    try {
      return new GenericDatumReader<GenericRecord>(schema)
          .read(null, DecoderFactory.get().jsonDecoder(schema, avroJson));
    } catch (IOException exception) {
      throw new JsonConversionException(exception);
    }
  }

  public static String convertRecordToJson(GenericRecord genericRecord) {
    var schema = genericRecord.getSchema();
    var baos = new ByteArrayOutputStream();
    try {
      var encoder = EncoderFactory.get().jsonEncoder(schema, baos);
      new GenericDatumWriter<GenericRecord>(schema).write(genericRecord, encoder);
      encoder.flush();
    } catch (IOException ioexp) {
      throw new JsonConversionException(ioexp);
    }

    return baos.toString();
  }

  /** A Runtime class to encapsulate any conversion exceptions/errors. */
  public static class JsonConversionException extends RuntimeException {

    public JsonConversionException(String message, Throwable cause) {
      super(message, cause);
    }

    public JsonConversionException(Throwable cause) {
      super(cause);
    }
  }
}
