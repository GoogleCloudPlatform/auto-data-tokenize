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

package com.google.cloud.solutions.autotokenize.testing;


import com.google.common.truth.extensions.proto.ProtoTruth;
import com.google.protobuf.Message;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** Provides a PAssert check for Proto Objects. */
public class CompareProtoIgnoringRepeatedFieldOrder<T extends Message>
    implements SerializableFunction<T, Void> {

  private final byte[] expectedItemBytes;
  private final Class<T> protoClass;

  public CompareProtoIgnoringRepeatedFieldOrder(T expectedItem) {
    this.expectedItemBytes = expectedItem.toByteArray();
    this.protoClass = (Class<T>) expectedItem.getClass();
  }

  private T expectedItem() {
    try {
      var builder = (Message.Builder) protoClass.getMethod("newBuilder").invoke(null);
      builder.mergeFrom(expectedItemBytes);
      return (T) builder.build();
    } catch (Exception exception) {
      throw new RuntimeException("error converting expected ProtoMessage");
    }
  }

  @Override
  public Void apply(T input) {
    ProtoTruth.assertThat(input).ignoringRepeatedFieldOrder().isEqualTo(expectedItem());
    return null;
  }
}
