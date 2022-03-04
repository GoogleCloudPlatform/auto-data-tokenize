/*
 * Copyright 2022 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.solutions.autotokenize.common.SecretsClient;
import com.google.cloud.solutions.autotokenize.encryptors.FixedClearTextKeySetExtractor;
import com.google.cloud.solutions.autotokenize.testing.TestResourceLoader;
import com.google.cloud.solutions.autotokenize.testing.stubs.kms.Base64DecodingKmsStub;
import com.google.cloud.solutions.autotokenize.testing.stubs.secretmanager.ConstantSecretVersionValueManagerServicesStub;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.common.truth.ThrowableSubject;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Consumer;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class EncryptionPipelineErrorTest {

  private static final ImmutableList<String> BASE_OPTIONS =
      ImmutableList.of(
          "--sourceType=AVRO",
          "--tokenizeColumns=$.kylosample.cc",
          "--tokenizeColumns=$.kylosample.email",
          "--schema="
              + TestResourceLoader.classPath()
                  .loadAsString("avro_records/userdata_records/schema.json"),
          "--tinkEncryptionKeySetJson="
              + TestResourceLoader.classPath().loadAsString("test_encryption_key.json"),
          "--mainKmsKeyUri=project/my-project/locations");

  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  public static ImmutableList<Object[]> makeOptions() {
    return ImmutableList.of(
        new Object[] {
          "No Input Provided",
          ImmutableList.of("--outputDirectory="),
          IllegalArgumentException.class,
        },
        new Object[] {
          "tink encryptor that does not extend ValueTokenizerFactory",
          ImmutableList.of("--outputDirectory=/dev/null", "--inputPattern="),
          IllegalArgumentException.class,
          "No output defined.%nProvide a GCS or BigQuery output"
        });
  }

  @Test
  public void run_noOutput_throwsIllegalArgumentException() {
    test(
        ImmutableList.of(),
        IllegalArgumentException.class,
        (throwable) ->
            throwable
                .hasMessageThat()
                .contains("No output defined.%nProvide a GCS or BigQuery output"));
  }

  @Test
  public void run_invalidEncryptor_throwsIllegalArgumentException() throws IOException {

    var inputFile =
        TestResourceLoader.classPath()
            .copyTo(temporaryFolder.newFolder())
            .createFileTestCopy("avro_records/nested_repeated/record.avro")
            .getAbsolutePath();

    test(
        ImmutableList.of(
            "--outputDirectory=" + temporaryFolder.newFolder().getAbsolutePath(),
            "--inputPattern=" + inputFile,
            "--valueTokenizerFactoryFullClassName=com.google.cloud.solutions.autotokenize.encryptors.WrongValueEncryptorFactory"),
        IllegalArgumentException.class,
        (throwableSubject) ->
            throwableSubject
                .hasCauseThat()
                .hasMessageThat()
                .contains(
                    "Class com.google.cloud.solutions.autotokenize.encryptors.WrongValueEncryptorFactory does extend ValueTokenizerFactory"));
  }

  public void test(
      ImmutableList<String> extraArgs,
      Class<? extends Exception> expectedException,
      Consumer<ThrowableSubject> checks) {

    var args = makeArgsWithExtraOptions(extraArgs);
    var options = PipelineOptionsFactory.fromArgs(args).as(EncryptionPipelineOptions.class);

    var pipeline =
        new EncryptionPipeline(
            options,
            TestPipeline.create(),
            null,
            makeSecretsClient(),
            KeyManagementServiceClient.create(new Base64DecodingKmsStub("")),
            new FixedClearTextKeySetExtractor(options.getTinkEncryptionKeySetJson()));

    var exception = assertThrows(expectedException, pipeline::run);

    checks.accept(assertThat(exception));
  }

  private static SecretsClient makeSecretsClient() {
    return SecretsClient.withSecretsStub(
        ConstantSecretVersionValueManagerServicesStub.of("resource/id/of/password/secret", ""));
  }

  private static String[] makeArgsWithExtraOptions(Collection<String> extraArgs) {
    return Streams.concat(BASE_OPTIONS.stream(), extraArgs.stream()).toArray(String[]::new);
  }
}
