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

package com.google.cloud.solutions.autotokenize.pipeline.encryptors;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.common.DeIdentifiedRecordSchemaConverter;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.common.flogger.StackSize;
import com.google.privacy.dlp.v2.Value;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * Tokenize specified columns of a record using {@link DaeadEncryptingValueTokenizer} to encrypt the
 * column's values.
 */
public final class EncryptingFlatRecordTokenizer implements FlatRecordTokenizer, Serializable {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final ImmutableSet<String> tokenizeSchemaKeys;
  private final DaeadEncryptingValueTokenizerFactory tokenizerFactory;

  /**
   * Instantiates the Record tokenizer with tokenize schema column names and the value tokenize
   * factory.
   *
   * @param tokenizeSchemaKeys the list of schema-keys that need to be tokenized.
   * @param tokenizerFactory the clear-text value tokenizer factory.
   */
  private EncryptingFlatRecordTokenizer(
      ImmutableSet<String> tokenizeSchemaKeys,
      DaeadEncryptingValueTokenizerFactory tokenizerFactory) {
    this.tokenizeSchemaKeys = tokenizeSchemaKeys;
    this.tokenizerFactory = tokenizerFactory;
  }

  public static EncryptingFlatRecordTokenizer withTokenizeSchemaKeys(
      ImmutableSet<String> tokenizeSchemaKeys) {
    return new EncryptingFlatRecordTokenizer(tokenizeSchemaKeys, null);
  }

  public EncryptingFlatRecordTokenizer withTokenizerFactory(
      DaeadEncryptingValueTokenizerFactory tokenizerFactory) {
    return new EncryptingFlatRecordTokenizer(tokenizeSchemaKeys, tokenizerFactory);
  }

  public SerializableFunction<FlatRecord, FlatRecord> encryptFn() {
    return this::encrypt;
  }

  @Override
  public FlatRecord encrypt(FlatRecord plainTextRecord) {
    checkNotNull(tokenizeSchemaKeys, "tokenizeSchemaKeys should not be null.");
    checkNotNull(tokenizerFactory, "tokenizeFactory should not be null.");

    FlatRecordTokenizer tokenizer = new FlatRecordTokenizer(
        new TokenizeColumnChecker(plainTextRecord));

    ImmutableMap<String, Value> updatedValues =
        plainTextRecord.getValuesMap().entrySet().stream()
            .map(tokenizer::processValue)
            .collect(ImmutableMap.toImmutableMap(ImmutablePair::getLeft, ImmutablePair::getRight));

    return FlatRecord.newBuilder().putAllValues(updatedValues)
        .build();//plainTextRecord.toBuilder().clearValues().putAllValues(updatedValues).build();
  }

  /**
   * Helper class to tokenize a flat record using the value tokenizer.
   */
  private class FlatRecordTokenizer {

    private final TokenizeColumnChecker columnChecker;
    private final DaeadEncryptingValueTokenizer valueTokenizer;

    private FlatRecordTokenizer(TokenizeColumnChecker columnChecker) {
      this.columnChecker = columnChecker;
      this.valueTokenizer = tokenizerFactory.makeValueTokenizer();
    }

    private ImmutablePair<String, Value> processValue(Map.Entry<String, Value> plainFlatEntry) {

      String flatKey = plainFlatEntry.getKey();
      Value plainValue = plainFlatEntry.getValue();

      try {
        if (columnChecker.isTokenizeColumn(flatKey)) {
          return ImmutablePair.of(
              columnChecker.encryptedColumnName(flatKey),
              plainValue.getTypeCase().equals(Value.TypeCase.TYPE_NOT_SET) ?
                  plainValue :
                  Value.newBuilder().setStringValue(valueTokenizer.encrypt(plainValue)).build());
        }
      } catch (ValueTokenizer.ValueTokenizingException valueTokenizingException) {
        logger.atSevere().withCause(valueTokenizingException).withStackTrace(StackSize.MEDIUM).log(
            "error encrypting value for flatKey: %s", flatKey);
      }
      return ImmutablePair.of(flatKey, plainValue);
    }
  }

  /**
   * Provides functionality to check a provided flat-record's columns and generate the name of the
   * encrypted column.
   */
  private class TokenizeColumnChecker {

    private final ImmutableMap<String, String> flatKeySchemaKey;

    private TokenizeColumnChecker(FlatRecord flatRecord) {
      this.flatKeySchemaKey = ImmutableMap.copyOf(flatRecord.getFlatKeySchemaMap());
    }

    /**
     * Returns {@code true} if the flatKey is present in the tokenize-schema-keys.
     */
    private boolean isTokenizeColumn(String flatKey) {
      return tokenizeSchemaKeys.contains(flatKeySchemaKey.get(flatKey));
    }

    /**
     * Generates the encrypted schema key for the a tokenize-column.
     * <p>
     * It identifies if the schemaKey is of an array-element and handles appropriately. e.g.
     * $.contacts[1].contact.number => $.contacts[1].contact.<b>encrypted_</b>number
     * </p>
     *
     * @param tokenizeFlatKey the column's schema key that needs to be converted to encrypted-name
     * @return the column's schema key with 'encrypted_' prefixed to the leaf field name.
     */
    private String encryptedColumnName(String tokenizeFlatKey) {
      String[] flatKeyParts = Splitter.on(".").splitToList(tokenizeFlatKey).toArray(new String[0]);
      String[] schemaKeyParts =
          Splitter.on(".").splitToList(flatKeySchemaKey.get(tokenizeFlatKey))
              .toArray(new String[0]);
      String encryptFieldName = schemaKeyParts[schemaKeyParts.length - 1];

      int fieldNameOffset = flatKeyParts[flatKeyParts.length - 1].equals(encryptFieldName) ? 1 : 2;

      int flatKeyFieldNamePartIndex = flatKeyParts.length - fieldNameOffset;

      //check for type conversion for union
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
