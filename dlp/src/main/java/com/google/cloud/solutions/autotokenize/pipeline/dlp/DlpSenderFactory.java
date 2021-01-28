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

package com.google.cloud.solutions.autotokenize.pipeline.dlp;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import com.google.privacy.dlp.v2.InfoType;
import java.io.IOException;
import java.io.Serializable;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Factory to instantiate DlpBatchSender.
 */
@AutoValue
public abstract class DlpSenderFactory implements Serializable {

  public abstract String projectId();

  @Nullable
  public abstract ImmutableSet<InfoType> observableType();

  public abstract DlpClientFactory dlpFactory();

  public DlpBatchInspect newSender() throws IOException {
    return new DlpBatchInspect(projectId(), observableType(), dlpFactory().newClient());
  }

  public static Builder builder() {
    return new AutoValue_DlpSenderFactory.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder projectId(String projectId);

    public abstract Builder observableType(@Nullable ImmutableSet<InfoType> observableType);

    public abstract Builder dlpFactory(DlpClientFactory dlpFactory);

    public abstract DlpSenderFactory build();
  }
}
