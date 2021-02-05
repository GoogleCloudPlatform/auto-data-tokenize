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

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.auto.value.AutoValue;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.Finding;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.InspectContentRequest;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.Likelihood;
import com.google.privacy.dlp.v2.Table;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.sdk.values.KV;

/** Sends the BatchTable to DLP API and makes the findings available as future result. */
final class DlpBatchInspect {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final String dlpProjectId;
  private final ImmutableSet<InfoType> observableTypes;
  private final DlpServiceClient dlpServiceClient;

  public DlpBatchInspect(
      String dlpProjectId,
      ImmutableSet<InfoType> observableTypes,
      DlpServiceClient dlpServiceClient) {
    this.dlpProjectId = dlpProjectId;
    this.observableTypes = observableTypes;
    this.dlpServiceClient = dlpServiceClient;
  }

  /**
   * Sends the sample content table to DLP and un-bundles the DlpIdentify response.
   *
   * @param dlpTableForIdentify a KV of sample data to identify infotype, as a structured DLP table
   *     and the map of flatrecord's value key and schema-key.
   * @return the column_name to information type map
   */
  public ImmutableList<KV<String, InfoType>> identifyInfoTypes(
      KV<Table, Map<String, String>> dlpTableForIdentify) {
    Table contentTable = dlpTableForIdentify.getKey();
    Map<String, String> flatKeySchemaKeyMap = dlpTableForIdentify.getValue();

    InspectContentResponse inspectContentResponse =
        dlpServiceClient.inspectContent(
            InspectContentRequest.newBuilder()
                .setParent(String.format("projects/%s", dlpProjectId))
                .setInspectConfig(buildInspectConfig())
                .setItem(ContentItem.newBuilder().setTable(contentTable).build())
                .build());

    logger.atInfo().log(
        "sending %s bytes containing %s records",
        contentTable.getSerializedSize(), contentTable.getRowsCount());

    FindingsTranslateFn findingsTranslate = FindingsTranslateFn.create(flatKeySchemaKeyMap);

    return inspectContentResponse.getResult().getFindingsList().stream()
        .map(findingsTranslate)
        .collect(toImmutableList());
  }

  private InspectConfig.Builder buildInspectConfig() {
    InspectConfig.Builder inspectionConfig =
        InspectConfig.newBuilder().setMinLikelihood(Likelihood.LIKELY);

    if (observableTypes != null && !observableTypes.isEmpty()) {
      inspectionConfig.addAllInfoTypes(observableTypes);
    }

    return inspectionConfig;
  }

  /**
   * Extracts the info-type information for flat-keys and emits as a KV of Schema-key -> InfoType
   */
  @AutoValue
  abstract static class FindingsTranslateFn implements Function<Finding, KV<String, InfoType>> {

    abstract Map<String, String> flatKeySchemaKeyMap();

    static FindingsTranslateFn create(Map<String, String> flatKeySchemaKeyMap) {
      return new AutoValue_DlpBatchInspect_FindingsTranslateFn(flatKeySchemaKeyMap);
    }

    @Override
    public KV<String, InfoType> apply(Finding finding) {
      String flatKey =
          finding
              .getLocation()
              .getContentLocationsList()
              .get(0)
              .getRecordLocation()
              .getFieldId()
              .getName();

      return KV.of(flatKeySchemaKeyMap().get(flatKey), finding.getInfoType());
    }
  }
}
