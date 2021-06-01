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

import static com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.privacy.dlp.v2.Value;
import java.io.Serializable;
import java.util.Collection;
import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * Provides functionality to check a provided flat-record's columns and generate the name of the
 * encrypted column.
 */
public class TokenizeColumnNameUpdater implements Serializable {

  private final ImmutableSet<String> tokenizeSchemaKeys;

  public TokenizeColumnNameUpdater(Collection<String> tokenizeSchemaKeys) {
    this.tokenizeSchemaKeys = ImmutableSet.copyOf(tokenizeSchemaKeys);
  }

  public FlatRecord updateColumnNames(FlatRecord genericRecord) {
    return new RecordColumnNameUpdater(tokenizeSchemaKeys, genericRecord).update();
  }

  public RecordColumnNameUpdater updaterFor(FlatRecord genericRecord) {
    return new RecordColumnNameUpdater(tokenizeSchemaKeys, genericRecord);
  }

  public static class RecordColumnNameUpdater implements Serializable {
    private final ImmutableSet<String> tokenizeSchemaKeys;
    private final FlatRecord genericRecord;
    private final ImmutableMap<String, String> flatKeySchemaKey;

    public RecordColumnNameUpdater(
        ImmutableSet<String> tokenizeSchemaKeys, FlatRecord genericRecord) {
      this.tokenizeSchemaKeys = tokenizeSchemaKeys;
      this.genericRecord = genericRecord;
      this.flatKeySchemaKey = ImmutableMap.copyOf(genericRecord.getFlatKeySchemaMap());
    }

    public FlatRecord update() {

      ImmutableMap<String, Value> updatedColumnNameWithValues =
          genericRecord.getValuesMap().entrySet().stream()
              .map(
                  entry -> {
                    if (isTokenizeColumn(entry.getKey())) {
                      return ImmutablePair.of(
                          encryptedColumnName(entry.getKey()), entry.getValue());
                    }

                    return ImmutablePair.of(entry.getKey(), entry.getValue());
                  })
              .collect(toImmutableMap(ImmutablePair::getLeft, ImmutablePair::getRight));

      return genericRecord.toBuilder()
          .clearValues()
          .putAllValues(updatedColumnNameWithValues)
          .build();
    }

    /** Returns {@code true} if the flatKey is present in the tokenize-schema-keys. */
    public boolean isTokenizeColumn(String flatKey) {
      return tokenizeSchemaKeys.contains(flatKeySchemaKey.get(flatKey));
    }

    /**
     * Generates the encrypted schema key for the a tokenize-column.
     *
     * <p>It identifies if the schemaKey is of an array-element and handles appropriately. e.g.
     * $.contacts[1].contact.number => $.contacts[1].contact.<b>encrypted_</b>number
     *
     * @param tokenizeFlatKey the column's schema key that needs to be converted to encrypted-name
     * @return the column's schema key with 'encrypted_' prefixed to the leaf field name.
     */
    public String encryptedColumnName(String tokenizeFlatKey) {
      String[] flatKeyParts = Splitter.on(".").splitToList(tokenizeFlatKey).toArray(new String[0]);
      String[] schemaKeyParts =
          Splitter.on(".")
              .splitToList(flatKeySchemaKey.get(tokenizeFlatKey))
              .toArray(new String[0]);
      String encryptFieldName = schemaKeyParts[schemaKeyParts.length - 1];

      int fieldNameOffset = flatKeyParts[flatKeyParts.length - 1].equals(encryptFieldName) ? 1 : 2;

      int flatKeyFieldNamePartIndex = flatKeyParts.length - fieldNameOffset;

      // check for type conversion for union
      if (fieldNameOffset == 2) {
        flatKeyParts[flatKeyFieldNamePartIndex + 1] = "string";
      }

      flatKeyParts[flatKeyFieldNamePartIndex] =
          DeIdentifiedRecordSchemaConverter.DEIDENTIFIED_FIELD_NAME_PREFIX
              + flatKeyParts[flatKeyFieldNamePartIndex];

      return Joiner.on(".").join(flatKeyParts);
    }
  }
}
