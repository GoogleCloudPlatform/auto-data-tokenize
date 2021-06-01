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

package com.google.cloud.solutions.autotokenize.encryptors;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import com.google.cloud.solutions.autotokenize.common.TokenizeColumnNameUpdater;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.common.flogger.StackSize;
import com.google.privacy.dlp.v2.Value;
import java.io.Serializable;
import java.util.Collection;
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
  private final TokenizeColumnNameUpdater tokenizeColumnNameUpdater;

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
    this.tokenizeColumnNameUpdater = new TokenizeColumnNameUpdater(tokenizeSchemaKeys);
  }

  public static EncryptingFlatRecordTokenizer withTokenizeSchemaKeys(
      Collection<String> tokenizeSchemaKeys) {
    return new EncryptingFlatRecordTokenizer(ImmutableSet.copyOf(tokenizeSchemaKeys), null);
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

    var tokenizer = new FlatRecordTokenizer(tokenizeColumnNameUpdater.updaterFor(plainTextRecord));

    ImmutableMap<String, Value> updatedValues =
        plainTextRecord.getValuesMap().entrySet().stream()
            .map(tokenizer::processValue)
            .collect(ImmutableMap.toImmutableMap(ImmutablePair::getLeft, ImmutablePair::getRight));

    return FlatRecord.newBuilder().putAllValues(updatedValues).build();
  }

  /** Helper class to tokenize a flat record using the value tokenizer. */
  private class FlatRecordTokenizer {

    private final TokenizeColumnNameUpdater.RecordColumnNameUpdater columnChecker;
    private final DaeadEncryptingValueTokenizer valueTokenizer;

    private FlatRecordTokenizer(TokenizeColumnNameUpdater.RecordColumnNameUpdater columnChecker) {
      this.columnChecker = columnChecker;
      this.valueTokenizer = tokenizerFactory.makeValueTokenizer();
    }

    private ImmutablePair<String, Value> processValue(Map.Entry<String, Value> plainFlatEntry) {

      var flatKey = plainFlatEntry.getKey();
      var plainValue = plainFlatEntry.getValue();

      try {
        if (columnChecker.isTokenizeColumn(flatKey)) {
          return ImmutablePair.of(
              columnChecker.encryptedColumnName(flatKey),
              plainValue.getTypeCase().equals(Value.TypeCase.TYPE_NOT_SET)
                  ? plainValue
                  : Value.newBuilder().setStringValue(valueTokenizer.encrypt(plainValue)).build());
        }
      } catch (ValueTokenizer.ValueTokenizingException valueTokenizingException) {
        logger.atSevere().withCause(valueTokenizingException).withStackTrace(StackSize.MEDIUM).log(
            "error encrypting value for flatKey: %s", flatKey);
      }
      return ImmutablePair.of(flatKey, plainValue);
    }
  }
}
