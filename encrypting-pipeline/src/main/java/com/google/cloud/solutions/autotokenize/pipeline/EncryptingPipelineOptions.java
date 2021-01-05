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

package com.google.cloud.solutions.autotokenize.pipeline;

import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Defines all the required inputs for the Encrypting pipeline.
 */
public interface EncryptingPipelineOptions extends GcpOptions {

  @Required
  String getFileType();

  void setFileType(String fileType);

  @Required
  String getInputFilePattern();

  void setInputFilePattern(String inputFilePattern);

  @Required
  String getSchema();

  void setSchema(String schema);

  @Required
  List<String> getTokenizeColumns();

  void setTokenizeColumns(List<String> fileDlpReportJson);

  @Required
  String getOutputDirectory();

  void setOutputDirectory(String outputFilePattern);

  @Required
  String getTinkEncryptionKeySetJson();

  void setTinkEncryptionKeySetJson(String tinkKeySetJson);

  @Required
  String getMainKmsKeyUri();

  void setMainKmsKeyUri(String mainKmsKeyUri);
}
