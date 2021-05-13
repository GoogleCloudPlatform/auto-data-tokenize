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


import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

/** Defines the sampling pipeline parameters. */
public interface DlpSamplerIdentifyOptions extends DataflowPipelineOptions {

  @Default.Integer(0)
  int getSampleSize();

  void setSampleSize(int sampleSize);

  @Validation.Required
  SourceType getSourceType();

  void setSourceType(SourceType fileType);

  @Validation.Required
  String getInputPattern();

  void setInputPattern(String inputFilePattern);

  @Validation.Required
  String getReportLocation();

  void setReportLocation(String reportLocation);

  List<String> getObservableInfoTypes();

  void setObservableInfoTypes(List<String> infoTypes);

  String getJdbcConnectionUrl();

  void setJdbcConnectionUrl(String jdbcConnectionUrl);

  String getJdbcDriverClass();

  void setJdbcDriverClass(String jdbcDriverClass);

  String getDataCatalogEntryGroupId();

  void setDataCatalogEntryGroupId(String dataCatalogEntryGroupId);

  String getDataCatalogInspectionTagTemplateId();

  void setDataCatalogInspectionTagTemplateId(String dataCatalogInspectionTagTemplateId);

  @Default.Boolean(false)
  boolean getDataCatalogForcedUpdate();

  void setDataCatalogForcedUpdate(boolean dataCatalogForcedUpdate);
}
