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

package com.google.cloud.solutions.autotokenize.pipeline.encryptors;

import com.google.privacy.dlp.v2.Value;

/**
 * Defines an interface for modules, that encrypt/decrypt values of {@link Value} objects to/from a
 * String.
 */
public interface ValueTokenizer {

  /**
   * Converts the value object to bytes and encrypt/tokenize them into a String.
   *
   * @param value the value object to tokenize.
   * @return A string representing Bas64 encoded value of the tokenized value object.
   * @throws ValueTokenizingException when unable to tokenize data.
   */
  String encrypt(Value value) throws ValueTokenizingException;

  Value decrypt(String cipherText) throws ValueTokenizingException;

  /**
   * Encapsulates any exception thrown during tokenization process.
   */
  class ValueTokenizingException extends Exception {

    public ValueTokenizingException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
