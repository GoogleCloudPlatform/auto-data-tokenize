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

package com.google.cloud.solutions.autotokenize.common;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.ColumnTransform;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.DlpEncryptConfig;
import com.google.common.collect.ImmutableList;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import java.util.Collection;
import java.util.stream.Stream;

public final class DeidentifyColumns {

  public static ImmutableList<String> columnNamesIn(DlpEncryptConfig dlpEncryptConfig) {
    return dlpEncryptConfig.getTransformsList().stream()
        .map(ColumnTransform::getColumnId)
        .collect(toImmutableList());
  }

  public static ImmutableList<String> columnNamesIn(Table dlpTable) {
    return columnNamesFromHeaders(dlpTable.getHeadersList());
  }

  public static ImmutableList<FieldId> fieldIdsFor(Collection<String> flatColumnNames) {
    return flatColumnNames.stream()
        .map(FieldId.newBuilder()::setName)
        .map(FieldId.Builder::build)
        .collect(toImmutableList());
  }

  public static ImmutableList<String> columnNamesFromHeaders(Collection<FieldId> fieldIds) {
    return fieldIds.stream().map(FieldId::getName).collect(toImmutableList());
  }

  public static ImmutableList<FieldId> fieldIdsFrom(Stream<String> columnNameStream) {
    return columnNameStream
        .map(FieldId.newBuilder()::setName)
        .map(FieldId.Builder::build)
        .collect(toImmutableList());
  }

  private DeidentifyColumns() {}
}
