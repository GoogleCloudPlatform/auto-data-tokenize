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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Boolean.logicalXor;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.JdbcConfiguration;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Validates PipelineOptions for SourceType=JDBC_TABLE. */
public final class JdbcConfigurationExtractor {

  private final AutoInspectAndTokenizeOptions options;

  public JdbcConfigurationExtractor(AutoInspectAndTokenizeOptions options) {
    this.options = options;
  }

  public static JdbcConfigurationExtractor using(AutoInspectAndTokenizeOptions options) {
    return new JdbcConfigurationExtractor(options);
  }

  /** Retuns a JdbcConfiguration object by parsing option arguments. */
  @Nullable
  public JdbcConfiguration jdbcConfiguration() {
    var sourceType = options.getSourceType();
    if (SourceType.JDBC_TABLE.equals(sourceType) || SourceType.JDBC_QUERY.equals(sourceType)) {

      checkArgument(
          isNotBlank(options.getJdbcConnectionUrl()) && isNotBlank(options.getJdbcDriverClass()),
          "Provide both jdbcDriverClass and jdbcConnectionUrl parameters.");
      checkArgument(isNotBlank(options.getJdbcUserName()), "jdbcUserName should not be blank.");
      checkArgument(
          logicalXor(
              options.getJdbcPassword() != null, isNotBlank(options.getJdbcPasswordSecretsKey())),
          "Provide jdbcPassword or jdbcPasswordSecretsKey, found none or both");

      var configBuilder =
          JdbcConfiguration.newBuilder()
              .setConnectionUrl(options.getJdbcConnectionUrl())
              .setDriverClassName(options.getJdbcDriverClass())
              .setUserName(options.getJdbcUserName());

      if (options.getJdbcFilterClause() != null) {
        configBuilder.setFilterClause(options.getJdbcFilterClause());
      }

      if (options.getJdbcPassword() != null) {
        configBuilder.setPassword(options.getJdbcPassword());
      } else {
        configBuilder.setPasswordSecretsKey(options.getJdbcPasswordSecretsKey());
      }

      return configBuilder.build();
    }

    return null;
  }
}
