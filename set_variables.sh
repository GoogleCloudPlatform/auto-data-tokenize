#!/bin/bash
#
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# The Google Cloud project to use for this tutorial
export PROJECT_ID="[YOUR_PROJECT_ID]"

# The Compute Engine region to use for running Dataflow jobs and create a
# temporary storage bucket
export REGION_ID="[YOUR_COMPUTE_ENGINE_REGION]"

# The Cloud Storage bucket to use as a temporary bucket for Dataflow
export TEMP_GCS_BUCKET="[CLOUD_STORAGE_BUCKET_NAME]"

# Name of the service account to use (not the email address)
# (e.g. tokenizing-runner)
export DLP_RUNNER_SERVICE_ACCOUNT_NAME="[SERVICE_ACCOUNT_NAME_FOR_RUNNER]"

# Fully Qualified Entry Group Id to use for creating/searching for Entries
# in Data Catalog for non-BigQuery entries.
# The ID must begin with a letter or underscore, contain only English letters, numbers and underscores, and be at most 64 characters.
# Refer: https://cloud.google.com/data-catalog/docs/reference/rest/v1/projects.locations.entryGroups/create#query-parameters
export DATA_CATALOG_ENTRY_GROUP_ID="[NON_BIGQUERY_DATA_CATALOG_ENTRY_GROUP]"

# The Data Catalog Tag Template Id to use
# for creating sensitivity tags in Data Catalog.
# The ID must contain only lowercase letters (a-z), numbers (0-9), or underscores (_), and must start with a letter or underscore. The maximum size is 64 bytes when encoded in UTF-8
# Refer: https://cloud.google.com/data-catalog/docs/reference/rest/v1/projects.locations.tagTemplates/create#query-parameters
export INSPECTION_TAG_TEMPLATE_ID="[DATA_CATALOG_TAG_TEMPLATE_NAME]"

# Name of the GCP KMS key ring name
export KMS_KEYRING_ID="[key-ring-name]"

# name of the symmetric Key encryption kms-key-id
export KMS_KEY_ID="[key-id]"

# The JSON file containing the TINK Wrapped data-key to use for encryption
export WRAPPED_KEY_FILE="[path-to-the-data-encryption-key-file]"

######################################
#      DON'T MODIFY LINES BELOW      #
######################################

# The GCP KMS resource URI
export MAIN_KMS_KEY_URI="gcp-kms://projects/${PROJECT_ID}/locations/${REGION_ID}/keyRings/${KMS_KEYRING_ID}/cryptoKeys/${KMS_KEY_ID}"

# The DLP Runner Service account email
DLP_RUNNER_SERVICE_ACCOUNT_EMAIL="${DLP_RUNNER_SERVICE_ACCOUNT_NAME}@$(echo $PROJECT_ID | awk -F':' '{print $2"."$1}' | sed 's/^\.//').iam.gserviceaccount.com"
export DLP_RUNNER_SERVICE_ACCOUNT_EMAIL

# Set an easy name to invoke the sampler module
AUTO_TOKENIZE_JAR="${PWD}/build/libs/autotokenize-all.jar"

# Fix the execution directory to present the JARs in this folder
# shellcheck disable=SC2139
alias sample_and_identify_pipeline="java -cp ${AUTO_TOKENIZE_JAR} com.google.cloud.solutions.autotokenize.pipeline.DlpInspectionPipeline"

# Fix the execution directory to present the JARs in this folder
# shellcheck disable=SC2139
alias tokenize_pipeline="java -cp ${AUTO_TOKENIZE_JAR} com.google.cloud.solutions.autotokenize.pipeline.EncryptionPipeline"

# shellcheck disable=SC2139
alias csv_tokenize_sorted="java -cp ${AUTO_TOKENIZE_JAR} com.google.cloud.solutions.autotokenize.pipeline.CsvTokenizationAndOrderingPipeline"