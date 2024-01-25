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

import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.KeyMaterialType;
import java.io.Serializable;

/** Factory interface to create new instances of a {@link ValueTokenizer}. */
public abstract class ValueTokenizerFactory implements Serializable {

  protected final String keyString;
  protected final KeyMaterialType keyMaterialType;

  public ValueTokenizerFactory(String keyString, KeyMaterialType keyMaterialType) {
    this.keyString = keyString;
    this.keyMaterialType = keyMaterialType;
  }

  /** Returns a new instance. */
  public abstract ValueTokenizer makeValueTokenizer();
}
