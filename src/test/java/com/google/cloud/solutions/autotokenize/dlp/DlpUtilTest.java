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

package com.google.cloud.solutions.autotokenize.dlp;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(Enclosed.class)
public final class DlpUtilTest {

  @RunWith(JUnit4.class)
  public static final class MakeDlpParentTest {

    @Test
    public void makeDlpParent_global() {
      assertThat(DlpUtil.makeDlpParent("some-project-id", "global"))
          .isEqualTo("projects/some-project-id");
    }

    @Test
    public void makeDlpParent_regionalLocation() {
      assertThat(DlpUtil.makeDlpParent("some-project-id", "us-central1"))
          .isEqualTo("projects/some-project-id/locations/us-central1");
    }
  }
}
