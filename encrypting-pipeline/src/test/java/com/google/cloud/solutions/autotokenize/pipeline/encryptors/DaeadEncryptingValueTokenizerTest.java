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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.DeterministicAead;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.daead.DeterministicAeadConfig;
import com.google.privacy.dlp.v2.Value;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class DaeadEncryptingValueTokenizerTest {

  private DeterministicAead testEncrypter;

  @Before
  public void setTestEncryptionKey() throws GeneralSecurityException, IOException {
    DeterministicAeadConfig.register();
    testEncrypter =
        CleartextKeysetHandle.read(
                JsonKeysetReader.withString(
                    TestResourceLoader.classPath().loadAsString("test_encryption_key.json")))
            .getPrimitive(DeterministicAead.class);
  }

  @Test
  public void encrypt_valid() throws ValueTokenizer.ValueTokenizingException {
    assertThat(
            new DaeadEncryptingValueTokenizer(testEncrypter)
                .encrypt(Value.newBuilder().setStringValue("this is sample string").build()))
        .isEqualTo("AWWfEcxShER0JUXMtNDppkQ83xhl4J/ZwU6QL/ExcJz93UD7mWOAxvPoKz8=");
  }

  @Test
  public void decrypt_valid() throws ValueTokenizer.ValueTokenizingException {
    assertThat(
            new DaeadEncryptingValueTokenizer(testEncrypter)
                .decrypt("AWWfEcxShER0JUXMtNDppkQ83xhl4J/ZwU6QL/ExcJz93UD7mWOAxvPoKz8="))
        .isEqualTo(Value.newBuilder().setStringValue("this is sample string").build());
  }
}
