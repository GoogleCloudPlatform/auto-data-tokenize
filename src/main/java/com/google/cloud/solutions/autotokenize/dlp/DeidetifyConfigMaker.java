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

package com.google.cloud.solutions.autotokenize.dlp;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnTransform;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.common.DeidentifyColumns;
import com.google.common.collect.Multimap;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InfoTypeTransformations;
import com.google.privacy.dlp.v2.InfoTypeTransformations.InfoTypeTransformation;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.RecordTransformations;
import java.util.regex.Pattern;

/**
 * Create DLP DeidentifyConfiguration based on user provided parameters. Uses the flattened record
 * keys' to schema keys and apply DLP primitive transforms appropriately.
 */
public final class DeidetifyConfigMaker {

  private static final Pattern ARRAY_INDEX_PATTERN = Pattern.compile("\\[\\d+\\]");

  private final DlpEncryptConfig dlpEncryptConfig;

  private DeidetifyConfigMaker(DlpEncryptConfig dlpEncryptConfig) {
    this.dlpEncryptConfig = dlpEncryptConfig;
  }

  public static DeidetifyConfigMaker of(DlpEncryptConfig dlpEncryptConfig) {
    return new DeidetifyConfigMaker(dlpEncryptConfig);
  }

  public DeidentifyConfig makeForMapping(Multimap<String, String> columnSchemaKeyMap) {

    var fieldTransforms =
        dlpEncryptConfig.getTransformsList().stream()
            .map(col -> new FieldTransformMaker(col, columnSchemaKeyMap).make())
            .filter(
                fieldTransformation ->
                    !FieldTransformation.getDefaultInstance().equals(fieldTransformation))
            .collect(toImmutableList());

    if (fieldTransforms.isEmpty()) {
      return DeidentifyConfig.getDefaultInstance();
    }

    return DeidentifyConfig.newBuilder()
        .setRecordTransformations(
            RecordTransformations.newBuilder().addAllFieldTransformations(fieldTransforms))
        .build();
  }

  /** Builds DLP DeIdentify Configuration based on ColumnTransform provided. */
  private static class FieldTransformMaker {
    private static final InfoType DEFAULT_SURROGATE_INFOTYPE =
        InfoType.newBuilder().setName("TOKENIZED_DATA").build();

    private final ColumnTransform colEncryptConfig;
    private final Multimap<String, String> columnSchemaKeyMap;

    private FieldTransformMaker(
        ColumnTransform colEncryptConfig, Multimap<String, String> columnSchemaKeyMap) {
      this.colEncryptConfig = colEncryptConfig;
      this.columnSchemaKeyMap = columnSchemaKeyMap;
    }

    private FieldTransformation make() {
      var fieldNames =
          columnSchemaKeyMap.get(colEncryptConfig.getColumnId()).stream()
              .map(colName -> ARRAY_INDEX_PATTERN.matcher(colName).replaceAll(""))
              .distinct()
              .collect(toImmutableList());

      if (fieldNames.isEmpty()) {
        return FieldTransformation.getDefaultInstance();
      }

      var fieldTransformBuilder =
          FieldTransformation.newBuilder().addAllFields(DeidentifyColumns.fieldIdsFor(fieldNames));

      if (colEncryptConfig.getFreeFormColumn()) {
        fieldTransformBuilder.setInfoTypeTransformations(
            InfoTypeTransformations.newBuilder()
                .addTransformations(buildInfoTypeTransformConfig()));
      } else {
        fieldTransformBuilder.setPrimitiveTransformation(createTransformation());
      }
      return fieldTransformBuilder.build();
    }

    private InfoTypeTransformation buildInfoTypeTransformConfig() {
      var infoTransformBuilder = InfoTypeTransformation.newBuilder();

      if (colEncryptConfig.getInfoTypesCount() > 0) {
        infoTransformBuilder.addAllInfoTypes(
            colEncryptConfig.getInfoTypesList().stream()
                .map(name -> InfoType.newBuilder().setName(name).build())
                .collect(toImmutableList()));
      }

      return infoTransformBuilder.setPrimitiveTransformation(createTransformation()).build();
    }

    /** Returns the transform with set surrogate_infotype value if missing. */
    private PrimitiveTransformation createTransformation() {
      var transformation = colEncryptConfig.getTransform();
      switch (transformation.getTransformationCase()) {
        case CRYPTO_REPLACE_FFX_FPE_CONFIG:
          return makeCryptoReplaceFfxWithSurrogate();

        case CRYPTO_DETERMINISTIC_CONFIG:
          return makeCryptoDeterministicWithSurrogate();

        default:
          return transformation;
      }
    }

    private PrimitiveTransformation makeCryptoReplaceFfxWithSurrogate() {
      var transformation = colEncryptConfig.getTransform();
      var cryptoFfxConfig = transformation.getCryptoReplaceFfxFpeConfig();
      if (cryptoFfxConfig.getSurrogateInfoType().equals(InfoType.getDefaultInstance())) {
        return PrimitiveTransformation.newBuilder()
            .setCryptoReplaceFfxFpeConfig(
                cryptoFfxConfig.toBuilder().setSurrogateInfoType(DEFAULT_SURROGATE_INFOTYPE))
            .build();
      }
      return transformation;
    }

    private PrimitiveTransformation makeCryptoDeterministicWithSurrogate() {
      var transformation = colEncryptConfig.getTransform();
      var cryptoConfig = transformation.getCryptoDeterministicConfig();
      if (cryptoConfig.getSurrogateInfoType().equals(InfoType.getDefaultInstance())) {
        return PrimitiveTransformation.newBuilder()
            .setCryptoDeterministicConfig(
                cryptoConfig.toBuilder().setSurrogateInfoType(DEFAULT_SURROGATE_INFOTYPE))
            .build();
      }

      return transformation;
    }
  }
}
