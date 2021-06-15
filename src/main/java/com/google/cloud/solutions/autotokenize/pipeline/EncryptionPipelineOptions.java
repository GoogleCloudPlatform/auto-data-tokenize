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


import java.util.List;
import org.apache.beam.sdk.options.Validation.Required;

/** Defines all the required inputs for the Encrypting pipeline. */
public interface EncryptionPipelineOptions extends AutoInspectAndTokenizeOptions {

  @Required
  String getSchema();

  void setSchema(String schema);

  @Required
  List<String> getTokenizeColumns();

  void setTokenizeColumns(List<String> fileDlpReportJson);

  String getOutputDirectory();

  void setOutputDirectory(String outputFilePattern);

  String getOutputBigQueryTable();

  void setOutputBigQueryTable(String outputBigQueryTable);

  String getTinkEncryptionKeySetJson();

  void setTinkEncryptionKeySetJson(String tinkKeySetJson);

  String getMainKmsKeyUri();

  void setMainKmsKeyUri(String mainKmsKeyUri);

  String getDlpEncryptConfigJson();

  void setDlpEncryptConfigJson(String dlpEncryptConfigJson);
}
