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
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation.Required;

/** Common options for Inspection and Tokenization pipelines. */
public interface AutoInspectAndTokenizeOptions extends UserEnvironmentOptions {
  @Required
  AutoTokenizeMessages.SourceType getSourceType();

  void setSourceType(AutoTokenizeMessages.SourceType fileType);

  @Required
  String getInputPattern();

  void setInputPattern(String inputPattern);

  @Default.String("global")
  String getDlpRegion();

  void setDlpRegion(String dlpRegion);

  // CsvIO parameters
  List<String> getCsvHeaders();

  void setCsvHeaders(List<String> csvHeaders);

  @Default.Boolean(false)
  boolean getCsvFirstRowHeader();

  void setCsvFirstRowHeader(boolean csvFirstRowHeader);

  @Default.String("UTF-8")
  String getCsvCharset();

  void setCsvCharset(String csvCharset);

  @Default.String(",")
  String getCsvColumnDelimiter();

  void setCsvColumnDelimiter(String csvColumnDelimiter);

  @Default.String("Default")
  String getCsvFormatType();

  void setCsvFormatType(String csvFormatType);

  // JDBC Connection parameters
  String getJdbcConnectionUrl();

  void setJdbcConnectionUrl(String jdbcConnectionUrl);

  String getJdbcDriverClass();

  void setJdbcDriverClass(String jdbcDriverClass);

  String getJdbcFilterClause();

  void setJdbcFilterClause(String jdbcFilterClause);

  String getJdbcUserName();

  void setJdbcUserName(String jdbcUserName);

  String getJdbcPasswordSecretsKey();

  void setJdbcPasswordSecretsKey(String jdbcPasswordSecretsKey);

  String getJdbcPassword();

  void setJdbcPassword(String jdbcPassword);

}
