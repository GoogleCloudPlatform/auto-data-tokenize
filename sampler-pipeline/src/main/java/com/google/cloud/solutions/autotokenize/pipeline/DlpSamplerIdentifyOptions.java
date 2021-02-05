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


import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Validation;

/** */
public interface DlpSamplerIdentifyOptions extends GcpOptions {

  @Validation.Required
  int getSampleSize();

  void setSampleSize(int sampleSize);

  @Validation.Required
  AutoTokenizeMessages.SourceType getSourceType();

  void setSourceType(AutoTokenizeMessages.SourceType fileType);

  @Validation.Required
  String getInputPattern();

  void setInputPattern(String inputFilePattern);

  @Validation.Required
  String getReportLocation();

  void setReportLocation(String reportLocation);

  List<String> getObservableInfoTypes();

  void setObservableInfoTypes(List<String> infoTypes);
}
