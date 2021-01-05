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

package com.google.cloud.solutions.autotokenize.common;

import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

import com.google.auto.value.AutoValue;
import com.google.common.base.MoreObjects;
import com.google.privacy.dlp.v2.Table;
import java.util.Map;
import org.apache.beam.sdk.values.KV;

/**
 * Represents Batch of elements as {@link Table} elements and its attributes.
 */
@AutoValue
public abstract class DlpTableBatch implements
    BatchAccumulator.Batch<KV<Table, Map<String, String>>> {

  /**
   * Returns number of columns in the table.
   */
  abstract int columns();

  /**
   * Returns number of rows excluding header row in the table.
   */
  abstract int rows();

  @Override
  public final String report() {
    return MoreObjects.toStringHelper(DlpTableBatch.class)
        .add("columns", columns())
        .add("rows", rows())
        .add("serializedSize", serializedSize())
        .toString();
  }

  public static DlpTableBatch create(KV<Table, Map<String, String>> batchValue) {
    int rows = firstNonNull(batchValue.getKey().getRowsCount(), 0);
    int columns = firstNonNull(batchValue.getKey().getHeadersCount(), 0);

    return new AutoValue_DlpTableBatch(batchValue, rows * columns,
        batchValue.getKey().getSerializedSize(), columns, rows);
  }
}
