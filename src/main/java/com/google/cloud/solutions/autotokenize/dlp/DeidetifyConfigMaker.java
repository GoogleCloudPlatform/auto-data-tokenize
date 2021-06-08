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
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.RecordTransformations;
import java.util.regex.Pattern;

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
            .collect(toImmutableList());

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
      return FieldTransformation.newBuilder()
          .addAllFields(
              DeidentifyColumns.fieldIdsFor(
                  columnSchemaKeyMap.get(colEncryptConfig.getColumnId()).stream()
                      .map(colName -> ARRAY_INDEX_PATTERN.matcher(colName).replaceAll(""))
                      .distinct()
                      .collect(toImmutableList())))
          .setPrimitiveTransformation(createTransformation())
          .build();
    }

    private PrimitiveTransformation createTransformation() {
      var transformation = colEncryptConfig.getTransform();

      if (transformation.hasCryptoReplaceFfxFpeConfig()) {
        var cryptoConfig = transformation.getCryptoReplaceFfxFpeConfig();
        if (cryptoConfig.getSurrogateInfoType().equals(InfoType.getDefaultInstance())) {
          return PrimitiveTransformation.newBuilder()
              .setCryptoReplaceFfxFpeConfig(
                  cryptoConfig.toBuilder().setSurrogateInfoType(DEFAULT_SURROGATE_INFOTYPE))
              .build();
        }

        return transformation;
      } else if (transformation.hasCryptoDeterministicConfig()) {
        var cryptoConfig = transformation.getCryptoDeterministicConfig();
        if (cryptoConfig.getSurrogateInfoType().equals(InfoType.getDefaultInstance())) {
          return PrimitiveTransformation.newBuilder()
              .setCryptoDeterministicConfig(
                  cryptoConfig.toBuilder().setSurrogateInfoType(DEFAULT_SURROGATE_INFOTYPE))
              .build();
        }

        return transformation;
      }

      return transformation;
    }
  }
}
