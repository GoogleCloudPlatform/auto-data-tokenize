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

import org.checkerframework.checker.nullness.qual.NonNull;

/** Implements simple DLP API related utility functions. */
public final class DlpUtil {

  /**
   * Returns the DLP API's processing location string in one of the formats: (a)
   * projects/PROJECT_ID/locations/LOCATION_ID (b) projects/PROJECT_ID
   *
   * @param dlpProjectId The Google Cloud DLP billing project id
   * @param dlpRegion the DLP processing region to use.
   */
  public static String makeDlpParent(@NonNull String dlpProjectId, @NonNull String dlpRegion) {
    var dlpRequestParent = String.format("projects/%s", dlpProjectId);

    if (!dlpRegion.equals("global")) {
      dlpRequestParent += String.format("/locations/%s", dlpRegion);
    }

    return dlpRequestParent;
  }

  private DlpUtil() {}
}
