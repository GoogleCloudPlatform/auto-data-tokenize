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


import com.google.auto.value.AutoValue;
import com.google.common.io.ByteSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class FileStringReader extends PTransform<PBegin, PCollection<String>> {

  abstract String filePattern();

  public static SingleOutput<ReadableFile, String> createReader() {
    return ParDo.of(new ReadFileAsStringFn());
  }

  public static FileStringReader create(String filePattern) {
    return new AutoValue_FileStringReader(filePattern);
  }

  @Override
  public PCollection<String> expand(PBegin input) {
    return input
        .apply(FileIO.match().filepattern(filePattern()))
        .apply(FileIO.readMatches())
        .apply(createReader());
  }

  public static final class ReadFileAsStringFn extends DoFn<ReadableFile, String> {

    @ProcessElement
    public void exportFileAsString(
        @Element ReadableFile file, OutputReceiver<String> outputReceiver) throws IOException {

      var fileSource =
          new ByteSource() {
            @Override
            public InputStream openStream() throws IOException {
              return Channels.newInputStream(file.open());
            }
          };

      var fileContentString =
          fileSource.asCharSource(StandardCharsets.UTF_8).lines().collect(Collectors.joining("\n"));

      outputReceiver.output(fileContentString);
    }
  }
}
