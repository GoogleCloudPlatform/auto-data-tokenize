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

package com.google.cloud.solutions.autotokenize.pipeline;


import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.KeyMaterialType;
import java.util.List;
import org.apache.beam.sdk.options.Default;

/** Defines all the required inputs for the Encrypting pipeline. */
public interface EncryptionPipelineOptions extends AutoInspectAndTokenizeOptions {

  String getSchema();

  void setSchema(String schema);

  String getSchemaLocation();

  void setSchemaLocation(String schema);

  List<String> getTokenizeColumns();

  void setTokenizeColumns(List<String> fileDlpReportJson);

  String getOutputDirectory();

  void setOutputDirectory(String outputFilePattern);

  String getOutputBigQueryTable();

  void setOutputBigQueryTable(String outputBigQueryTable);

  @Default.Boolean(false)
  boolean getBigQueryAppend();

  void setBigQueryAppend(boolean bigQueryAppend);

  String getTinkEncryptionKeySetJson();

  void setTinkEncryptionKeySetJson(String tinkKeySetJson);

  String getMainKmsKeyUri();

  void setMainKmsKeyUri(String mainKmsKeyUri);

  String getDlpEncryptConfigJson();

  void setDlpEncryptConfigJson(String dlpEncryptConfigJson);

  @Default.String(
      "com.google.cloud.solutions.autotokenize.encryptors.DaeadEncryptingValueTokenizerFactory")
  String getValueTokenizerFactoryFullClassName();

  void setValueTokenizerFactoryFullClassName(String valueTokenizerFactoryFullClassName);

  String getKeyMaterial();

  void setKeyMaterial(String keyMaterial);

  @Default.InstanceFactory(KeyMaterialTypeFactory.class)
  KeyMaterialType getKeyMaterialType();

  void setKeyMaterialType(KeyMaterialType keyMaterialType);
}
