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

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnTransform;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.cloud.solutions.autotokenize.common.JsonConvertor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.flogger.GoogleLogger;
import com.google.common.truth.extensions.proto.ProtoTruth;
import com.google.privacy.dlp.v2.CryptoDeterministicConfig;
import com.google.privacy.dlp.v2.CryptoHashConfig;
import com.google.privacy.dlp.v2.CryptoKey;
import com.google.privacy.dlp.v2.CryptoReplaceFfxFpeConfig;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InfoTypeTransformations;
import com.google.privacy.dlp.v2.InfoTypeTransformations.InfoTypeTransformation;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.RecordTransformations;
import com.google.privacy.dlp.v2.UnwrappedCryptoKey;
import com.google.protobuf.ByteString;
import java.util.Base64;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DeidetifyConfigMakerTest {

  private static final CryptoKey UNWRAPPED_CRYPTO_KEY =
      CryptoKey.newBuilder()
          .setUnwrapped(
              UnwrappedCryptoKey.newBuilder()
                  .setKey(
                      ByteString.copyFrom(
                          Base64.getDecoder()
                              .decode("QiZFKUhATWNRZlRqV21acTR0N3cheiVDKkYtSmFOZFI="))))
          .build();
  private static final ImmutableMultimap<String, String> SCHEMA_KEY_FLAT_KEY_MAP =
      ImmutableMultimap.<String, String>builder()
          .put(
              "$.contact_records.contacts.[\"contact\"].number",
              "$.contacts[1].[\"contact\"].number")
          .put(
              "$.contact_records.contacts.[\"contact\"].number",
              "$.contacts[0].[\"contact\"].number")
          .put(
              "$.contact_records.contacts.[\"contact\"].number",
              "$.contacts[2].[\"contact\"].number")
          .put("$.contact_records.emails", "$.emails[0]")
          .put("$.contact_records.name", "$.name")
          .put("$.contact_records.chat_line", "$.chat_line[0]")
          .put("$.contact_records.chat_line", "$.chat_line[1]")
          .put("$.contact_records.chat_line", "$.chat_line[2]")
          .build();

  private final DlpEncryptConfig dlpEncryptConfig;
  private final DeidentifyConfig expectedConfiguration;

  public DeidetifyConfigMakerTest(
      String testConditionName,
      DlpEncryptConfig dlpEncryptConfig,
      DeidentifyConfig expectedConfiguration) {
    this.dlpEncryptConfig = dlpEncryptConfig;
    this.expectedConfiguration = expectedConfiguration;
  }

  @Test
  public void makeForMapping_valid() {
    var configMaker = DeidetifyConfigMaker.of(dlpEncryptConfig);
    var actualConfig = configMaker.makeForMapping(SCHEMA_KEY_FLAT_KEY_MAP);

    GoogleLogger.forEnclosingClass()
        .atInfo()
        .log(
            "Actual:%s%nExpected:%s",
            JsonConvertor.asJsonString(actualConfig),
            JsonConvertor.asJsonString(expectedConfiguration));

    ProtoTruth.assertThat(actualConfig)
        .ignoringRepeatedFieldOrder()
        .isEqualTo(expectedConfiguration);
  }

  @Parameters(name = "{0}")
  public static ImmutableList<Object[]> testParameters() {
    return ImmutableList.<Object[]>builder()
        .add(
            new Object[] {
              "Adds SurrogateInfoType toTransform (CryptoDeterministic)",
              DlpEncryptConfig.newBuilder()
                  .addTransforms(
                      ColumnTransform.newBuilder()
                          .setColumnId("$.contact_records.contacts.[\"contact\"].number")
                          .setTransform(
                              PrimitiveTransformation.newBuilder()
                                  .setCryptoDeterministicConfig(
                                      CryptoDeterministicConfig.newBuilder()
                                          .setCryptoKey(UNWRAPPED_CRYPTO_KEY))
                                  .build()))
                  .build(),
              DeidentifyConfig.newBuilder()
                  .setRecordTransformations(
                      RecordTransformations.newBuilder()
                          .addFieldTransformations(
                              FieldTransformation.newBuilder()
                                  .addFields(
                                      FieldId.newBuilder()
                                          .setName("$.contacts.[\"contact\"].number"))
                                  .setPrimitiveTransformation(
                                      PrimitiveTransformation.newBuilder()
                                          .setCryptoDeterministicConfig(
                                              CryptoDeterministicConfig.newBuilder()
                                                  .setCryptoKey(UNWRAPPED_CRYPTO_KEY)
                                                  .setSurrogateInfoType(
                                                      InfoType.newBuilder()
                                                          .setName("TOKENIZED_DATA"))))))
                  .build()
            })
        .add(
            new Object[] {
              "Does not replace user specified SurrogateInfoType (CryptoDeterministic)",
              DlpEncryptConfig.newBuilder()
                  .addTransforms(
                      ColumnTransform.newBuilder()
                          .setColumnId("$.contact_records.contacts.[\"contact\"].number")
                          .setTransform(
                              PrimitiveTransformation.newBuilder()
                                  .setCryptoDeterministicConfig(
                                      CryptoDeterministicConfig.newBuilder()
                                          .setCryptoKey(UNWRAPPED_CRYPTO_KEY)
                                          .setSurrogateInfoType(
                                              InfoType.newBuilder().setName("USER_TYPE")))))
                  .build(),
              DeidentifyConfig.newBuilder()
                  .setRecordTransformations(
                      RecordTransformations.newBuilder()
                          .addFieldTransformations(
                              FieldTransformation.newBuilder()
                                  .addFields(
                                      FieldId.newBuilder()
                                          .setName("$.contacts.[\"contact\"].number"))
                                  .setPrimitiveTransformation(
                                      PrimitiveTransformation.newBuilder()
                                          .setCryptoDeterministicConfig(
                                              CryptoDeterministicConfig.newBuilder()
                                                  .setCryptoKey(UNWRAPPED_CRYPTO_KEY)
                                                  .setSurrogateInfoType(
                                                      InfoType.newBuilder()
                                                          .setName("USER_TYPE"))))))
                  .build()
            })
        .add(
            new Object[] {
              "Adds SurrogateInfoType toTransform (CryptoFfx)",
              DlpEncryptConfig.newBuilder()
                  .addTransforms(
                      ColumnTransform.newBuilder()
                          .setColumnId("$.contact_records.contacts.[\"contact\"].number")
                          .setTransform(
                              PrimitiveTransformation.newBuilder()
                                  .setCryptoReplaceFfxFpeConfig(
                                      CryptoReplaceFfxFpeConfig.newBuilder()
                                          .setCryptoKey(UNWRAPPED_CRYPTO_KEY))
                                  .build()))
                  .build(),
              DeidentifyConfig.newBuilder()
                  .setRecordTransformations(
                      RecordTransformations.newBuilder()
                          .addFieldTransformations(
                              FieldTransformation.newBuilder()
                                  .addFields(
                                      FieldId.newBuilder()
                                          .setName("$.contacts.[\"contact\"].number"))
                                  .setPrimitiveTransformation(
                                      PrimitiveTransformation.newBuilder()
                                          .setCryptoReplaceFfxFpeConfig(
                                              CryptoReplaceFfxFpeConfig.newBuilder()
                                                  .setCryptoKey(UNWRAPPED_CRYPTO_KEY)
                                                  .setSurrogateInfoType(
                                                      InfoType.newBuilder()
                                                          .setName("TOKENIZED_DATA"))))))
                  .build()
            })
        .add(
            new Object[] {
              "Does not replace user specified SurrogateInfoType (CryptoFfx)",
              DlpEncryptConfig.newBuilder()
                  .addTransforms(
                      ColumnTransform.newBuilder()
                          .setColumnId("$.contact_records.contacts.[\"contact\"].number")
                          .setTransform(
                              PrimitiveTransformation.newBuilder()
                                  .setCryptoReplaceFfxFpeConfig(
                                      CryptoReplaceFfxFpeConfig.newBuilder()
                                          .setCryptoKey(UNWRAPPED_CRYPTO_KEY)
                                          .setSurrogateInfoType(
                                              InfoType.newBuilder().setName("USER_TYPE")))))
                  .build(),
              DeidentifyConfig.newBuilder()
                  .setRecordTransformations(
                      RecordTransformations.newBuilder()
                          .addFieldTransformations(
                              FieldTransformation.newBuilder()
                                  .addFields(
                                      FieldId.newBuilder()
                                          .setName("$.contacts.[\"contact\"].number"))
                                  .setPrimitiveTransformation(
                                      PrimitiveTransformation.newBuilder()
                                          .setCryptoReplaceFfxFpeConfig(
                                              CryptoReplaceFfxFpeConfig.newBuilder()
                                                  .setCryptoKey(UNWRAPPED_CRYPTO_KEY)
                                                  .setSurrogateInfoType(
                                                      InfoType.newBuilder()
                                                          .setName("USER_TYPE"))))))
                  .build()
            })
        .add(
            new Object[] {
              "Does not add surrogateType for non-CryptoFfx/CryptoDeterministic",
              DlpEncryptConfig.newBuilder()
                  .addTransforms(
                      ColumnTransform.newBuilder()
                          .setColumnId("$.contact_records.contacts.[\"contact\"].number")
                          .setTransform(
                              PrimitiveTransformation.newBuilder()
                                  .setCryptoHashConfig(
                                      CryptoHashConfig.newBuilder()
                                          .setCryptoKey(UNWRAPPED_CRYPTO_KEY))))
                  .build(),
              DeidentifyConfig.newBuilder()
                  .setRecordTransformations(
                      RecordTransformations.newBuilder()
                          .addFieldTransformations(
                              FieldTransformation.newBuilder()
                                  .addFields(
                                      FieldId.newBuilder()
                                          .setName("$.contacts.[\"contact\"].number"))
                                  .setPrimitiveTransformation(
                                      PrimitiveTransformation.newBuilder()
                                          .setCryptoHashConfig(
                                              CryptoHashConfig.newBuilder()
                                                  .setCryptoKey(UNWRAPPED_CRYPTO_KEY)))))
                  .build()
            })
        .add(
            new Object[] {
              "columnIdNotMatched - EmptyConfig",
              DlpEncryptConfig.newBuilder()
                  .addTransforms(
                      ColumnTransform.newBuilder()
                          .setFreeFormColumn(true)
                          .addInfoTypes("PERSON_NAME")
                          .addInfoTypes("PHONE_NUMBER")
                          .setColumnId("$.contact_records.unknown_field")
                          .setTransform(
                              PrimitiveTransformation.newBuilder()
                                  .setCryptoDeterministicConfig(
                                      CryptoDeterministicConfig.newBuilder()
                                          .setCryptoKey(UNWRAPPED_CRYPTO_KEY))
                                  .build()))
                  .build(),
              DeidentifyConfig.getDefaultInstance()
            })
        .add(
            new Object[] {
              "FreeFormText - Handles InfoTypeTransforms",
              DlpEncryptConfig.newBuilder()
                  .addTransforms(
                      ColumnTransform.newBuilder()
                          .setFreeFormColumn(true)
                          .addInfoTypes("PERSON_NAME")
                          .addInfoTypes("PHONE_NUMBER")
                          .setColumnId("$.contact_records.chat_line")
                          .setTransform(
                              PrimitiveTransformation.newBuilder()
                                  .setCryptoDeterministicConfig(
                                      CryptoDeterministicConfig.newBuilder()
                                          .setCryptoKey(UNWRAPPED_CRYPTO_KEY))
                                  .build()))
                  .build(),
              DeidentifyConfig.newBuilder()
                  .setRecordTransformations(
                      RecordTransformations.newBuilder()
                          .addFieldTransformations(
                              FieldTransformation.newBuilder()
                                  .addFields(FieldId.newBuilder().setName("$.chat_line"))
                                  .setInfoTypeTransformations(
                                      InfoTypeTransformations.newBuilder()
                                          .addTransformations(
                                              InfoTypeTransformation.newBuilder()
                                                  .addInfoTypes(
                                                      InfoType.newBuilder().setName("PHONE_NUMBER"))
                                                  .addInfoTypes(
                                                      InfoType.newBuilder().setName("PERSON_NAME"))
                                                  .setPrimitiveTransformation(
                                                      PrimitiveTransformation.newBuilder()
                                                          .setCryptoDeterministicConfig(
                                                              CryptoDeterministicConfig.newBuilder()
                                                                  .setCryptoKey(
                                                                      UNWRAPPED_CRYPTO_KEY)
                                                                  .setSurrogateInfoType(
                                                                      InfoType.newBuilder()
                                                                          .setName(
                                                                              "TOKENIZED_DATA"))))))))
                  .build()
            })
        .build();
  }
}
