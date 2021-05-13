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

import static com.google.cloud.solutions.autotokenize.common.AvroSchemaToCatalogSchema.convertToCatalogSchemaMapping;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.common.truth.extensions.proto.ProtoTruth;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class AvroSchemaToCatalogSchemaTest {

  private final String testConditionName;
  private final String avroSchemaFile;
  private final String dataCatalogSchemaFile;
  private final ImmutableMap<String, String> expectedFlatKeyMap;

  public AvroSchemaToCatalogSchemaTest(
      String testConditionName,
      String avroSchemaFile,
      String dataCatalogSchemaFile,
      ImmutableMap<String, String> expectedFlatKeyMap) {
    this.testConditionName = testConditionName;
    this.avroSchemaFile = avroSchemaFile;
    this.dataCatalogSchemaFile = dataCatalogSchemaFile;
    this.expectedFlatKeyMap = expectedFlatKeyMap;
  }

  @Test
  public void convertToCatalogSchemaMapping_valid() {

    var catalogSchemaAndFlatKeys =
        convertToCatalogSchemaMapping(
            TestResourceLoader.classPath().forAvro().asSchema(avroSchemaFile));

    ProtoTruth.assertThat(catalogSchemaAndFlatKeys.getCatalogSchema())
        .ignoringRepeatedFieldOrder()
        .isEqualTo(
            TestResourceLoader.classPath()
                .forProto(com.google.cloud.datacatalog.v1.Schema.class)
                .loadText(dataCatalogSchemaFile));

    assertThat(catalogSchemaAndFlatKeys.getFlatSchemaKeyMappingMap())
        .containsExactlyEntriesIn(expectedFlatKeyMap);
  }

  @Parameters(name = "{0}")
  public static ImmutableList<Object[]> testParameters() {
    return ImmutableList.<Object[]>builder()
        .add(
            new Object[] {
              "Simple Schema",
              "avro_records/simple_field_avro_schema.json",
              "catalog_schema_items/simple_field_avro_schema.textpb",
              loadJsonMap("catalog_schema_items/simple_field_flat_keys.json")
            })
        .add(
            new Object[] {
              "Union with all types",
              "avro_records/union_with_all_types_avro_schema.json",
              "catalog_schema_items/union_with_all_types_avro_schema.textpb",
              loadJsonMap("catalog_schema_items/union_with_all_types_flat_keys.json")
            })
        .add(
            new Object[] {
              "Array with Union type",
              "avro_records/array_with_null_union_record_avro_schema.json",
              "catalog_schema_items/array_with_null_union_record_avro_schema.textpb",
              loadJsonMap("catalog_schema_items/array_with_null_union_record_flat_keys.json")
            })
        .add(
            new Object[] {
              "Nested Records",
              "catalog_schema_items/avro_schema_with_nested_fields.json",
              "catalog_schema_items/datacatalog_nested_field_schema.textpb",
              loadJsonMap("catalog_schema_items/nested_field_schema_flat_keys.json")
            })
        .add(
            new Object[] {
              "Nested Repeated Records",
              "catalog_schema_items/nested_repeated_field_avro_schema.json",
              "catalog_schema_items/nested_repeated_field_datacatalog_schema.textpb",
              loadJsonMap("catalog_schema_items/nested_repeated_field_schema_flat_keys.json")
            })
        .build();
  }

  private static ImmutableMap<String, String> loadJsonMap(String resourceUri) {
    var mapType = (new TypeToken<Map<String, String>>() {}).getType();
    return ImmutableMap.copyOf(
        (Map<String, String>) TestResourceLoader.classPath().loadAsJson(resourceUri, mapType));
  }
}
