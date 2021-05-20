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

package com.google.cloud.solutions.autotokenize.pipeline.datacatalog;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.Function.identity;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.auto.value.AutoValue;
import com.google.cloud.datacatalog.v1.CreateEntryRequest;
import com.google.cloud.datacatalog.v1.DataCatalogClient;
import com.google.cloud.datacatalog.v1.DataCatalogClient.ListTagsPage;
import com.google.cloud.datacatalog.v1.Entry;
import com.google.cloud.datacatalog.v1.EntryName;
import com.google.cloud.datacatalog.v1.LookupEntryRequest;
import com.google.cloud.datacatalog.v1.Tag;
import com.google.cloud.datacatalog.v1.stub.DataCatalogStub;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.SourceType;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.UpdatableDataCatalogItems;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import com.google.protobuf.FieldMask;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoValue
public abstract class DataCatalogWriter
    extends PTransform<PCollection<UpdatableDataCatalogItems>, PCollection<Void>> {

  abstract String inspectionTagTemplateId();

  abstract boolean forceUpdate();

  abstract @Nullable String entryGroupId();

  abstract @Nullable DataCatalogStub stub();

  abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setEntryGroupId(String value);

    public abstract Builder setInspectionTagTemplateId(String inspectionTagTemplateId);

    public abstract Builder setForceUpdate(boolean forceUpdate);

    @VisibleForTesting
    abstract Builder setStub(DataCatalogStub value);

    abstract DataCatalogWriter autoBuild();

    public DataCatalogWriter build() {
      var tempWriter = autoBuild();

      // Validate entry group id
      var entryGroupId = tempWriter.entryGroupId();
      checkArgument(
          entryGroupId == null
              || entryGroupId.matches("^projects/[^/]+/locations/[^/]+/entryGroups/[^/]+$"),
          "Data Catalog entry group id does not match pattern: ^projects\\/[^/]+\\/locations\\/[^/]+\\/entryGroups\\/[^/]+$ found:%s",
          entryGroupId);

      return tempWriter;
    }
  }

  public static Builder builder() {
    return new AutoValue_DataCatalogWriter.Builder().setForceUpdate(false);
  }

  @VisibleForTesting
  DataCatalogWriter withStub(DataCatalogStub stub) {
    return toBuilder().setStub(stub).build();
  }

  @Override
  public PCollection<Void> expand(PCollection<UpdatableDataCatalogItems> input) {
    return input.apply(
        "WriteUpdatesToCatalog",
        ParDo.of(
            DataCatalogWriterFn.builder()
                .setEntryGroupId(entryGroupId())
                .setForceUpdate(forceUpdate())
                .setInspectionTagTemplateId(inspectionTagTemplateId())
                .build()));
  }

  @AutoValue
  abstract static class DataCatalogWriterFn extends DoFn<UpdatableDataCatalogItems, Void> {

    private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

    abstract String inspectionTagTemplateId();

    abstract boolean forceUpdate();

    abstract @Nullable String entryGroupId();

    abstract @Nullable DataCatalogStub stub();

    private transient DataCatalogClient catalogClient;

    @Setup
    public void buildCatalogClient() throws IOException {
      if (catalogClient == null) {
        catalogClient = buildClient();
      }
    }

    @Teardown
    public void destroyCatalogClient() throws InterruptedException {
      if (catalogClient != null) {
        catalogClient.close();
        catalogClient.shutdown();
        catalogClient.awaitTermination(5, TimeUnit.MINUTES);
      }
    }

    @ProcessElement
    public void writeUpdatesToDataCatalog(@Element UpdatableDataCatalogItems updateItems) {
      new EntryProcessor(updateItems)
          .processEntry()
          .ifPresent(entry -> new TagsProcessor(entry, updateItems.getTagsList()).updateTags());
    }

    private DataCatalogClient buildClient() throws IOException {
      return (stub() == null) ? DataCatalogClient.create() : DataCatalogClient.create(stub());
    }

    private static Builder builder() {
      return new AutoValue_DataCatalogWriter_DataCatalogWriterFn.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setInspectionTagTemplateId(String value);

      public abstract Builder setForceUpdate(boolean value);

      public abstract Builder setEntryGroupId(String value);

      public abstract Builder setStub(DataCatalogStub value);

      public abstract DataCatalogWriterFn build();
    }

    private class EntryProcessor {

      private final SourceType sourceType;
      private final String inputPattern;
      private final @Nullable Entry entryToCreate;

      public EntryProcessor(UpdatableDataCatalogItems updatableDataCatalogItems) {
        this.sourceType = updatableDataCatalogItems.getSourceType();
        this.inputPattern = updatableDataCatalogItems.getInputPattern();
        this.entryToCreate = updatableDataCatalogItems.getEntry();
      }

      public Optional<Entry> processEntry() {
        var existingEntry = getExistingEntry();

        if (existingEntry.isEmpty() && sourceType.equals(SourceType.BIGQUERY_TABLE)) {
          throw new UnsupportedOperationException("Missing BigQuery Table entry");
        }

        if (existingEntry.isPresent()
            && (sourceType.equals(SourceType.BIGQUERY_TABLE) || !forceUpdate())) {
          return existingEntry;
        }

        if (entryToCreate != null) {

          if (existingEntry.isPresent() && forceUpdate()) {
            catalogClient.deleteEntry(existingEntry.get().getName());
          }

          return Optional.of(
              catalogClient.createEntry(
                  CreateEntryRequest.newBuilder()
                      .setParent(entryGroupId())
                      .setEntryId(makeNonBigQueryEntryId())
                      .setEntry(entryToCreate)
                      .build()));
        }

        return Optional.empty();
      }

      private Optional<Entry> getExistingEntry() {
        try {
          if (sourceType.equals(SourceType.BIGQUERY_TABLE)) {
            return Optional.ofNullable(
                catalogClient.lookupEntry(
                    LookupEntryRequest.newBuilder()
                        .setLinkedResource(makeBigQueryTableLinkedResource())
                        .build()));
          }

          return Optional.ofNullable(
              catalogClient.getEntry(
                  EntryName.parse(
                      String.format("%s/entries/%s", entryGroupId(), makeNonBigQueryEntryId()))));

        } catch (Exception ex) {
          logger.atInfo().withCause(ex).log(
              "error retrieving entry: (%s) %s", sourceType, inputPattern);
          return Optional.empty();
        }
      }

      private String makeNonBigQueryEntryId() {
        return inputPattern.replaceAll("[^\\w]", "_").replaceAll("[_]+", "_");
      }

      private String makeBigQueryTableLinkedResource() {
        return Pattern.compile("^(?<projectId>.+)\\:(?<datasetId>\\w+)\\.(?<tableId>\\w+)$")
            .matcher(inputPattern)
            .replaceAll(
                "//bigquery.googleapis.com/projects/${projectId}/datasets/${datasetId}/tables/${tableId}");
      }
    }

    private class TagsProcessor {

      private final String targetEntryId;
      private final ImmutableList<Tag> tagsToApply;
      private final ImmutableMap<String, Tag> existingTags;

      public TagsProcessor(Entry targetEntry, List<Tag> tagsToApply) {
        this.targetEntryId = targetEntry.getName();
        this.tagsToApply = ImmutableList.copyOf(tagsToApply);
        this.existingTags = getExistingTags();
      }

      public void updateTags() {
        for (var tag : tagsToApply) {
          if (isNotBlank(tag.getName())) {
            logger.atInfo().log("skipping tag: name should be blank, found (%s)", tag.getName());
            continue;
          }

          if (existingTags.containsKey(tag.getColumn())) {
            updateTag(tag);
          } else {
            catalogClient.createTag(targetEntryId, tag);
          }
        }
      }

      private void updateTag(Tag tag) {
        if (forceUpdate()) {
          catalogClient.updateTag(
              Tag.newBuilder(existingTags.get(tag.getColumn())).mergeFrom(tag).build(),
              // Update all fields
              FieldMask.getDefaultInstance());
        } else {
          logger.atInfo().log("skipping tag for %s(:%s)", targetEntryId, tag.getColumn());
        }
      }

      private ImmutableMap<String, Tag> getExistingTags() {
        return StreamSupport.stream(
                catalogClient.listTags(targetEntryId).iteratePages().spliterator(),
                /*parallel=*/ false)
            .map(ListTagsPage::getValues)
            .flatMap(
                tagsIterator ->
                    StreamSupport.stream(tagsIterator.spliterator(), /*parallel=*/ false))
            .filter(tag -> inspectionTagTemplateId().equals(tag.getTemplate()))
            .collect(toImmutableMap(Tag::getColumn, identity()));
      }
    }
  }
}
