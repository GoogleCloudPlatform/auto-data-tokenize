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
export PROJECT_ID="<your-project-id>"

# The Compute Engine region to use for running Dataflow jobs and create a
# temporary storage bucket
export REGION_ID="<compute-engine-region>"

# define the GCS bucket to use as temporary bucket for Dataflow
export TEMP_GCS_BUCKET="<name-of-the-bucket>"

# Name of the service account to use (not the email address)
export DLP_RUNNER_SERVICE_ACCOUNT_NAME="<service-account-name-for-runner>"

# Name of the GCP KMS key ring name
export KMS_KEYRING_ID="<key-ring-name>"

# name of the symmetric Key encryption kms-key-id
export KMS_KEY_ID="<key-id>"

# The JSON file containing the TINK Wrapped data-key to use for encryption
export WRAPPED_KEY_FILE="<path-to-the-data-encryption-key-file>"

######################################
#      DON'T MODIFY LINES BELOW      #
######################################

# The GCP KMS resource URI
export MAIN_KMS_KEY_URI="gcp-kms://projects/${PROJECT_ID}/locations/${REGION_ID}/keyRings/${KMS_KEYRING_ID}/cryptoKeys/${KMS_KEY_ID}"

# The DLP Runner Service account email
export DLP_RUNNER_SERVICE_ACCOUNT_EMAIL="${DLP_RUNNER_SERVICE_ACCOUNT_NAME}@$(echo $PROJECT_ID | awk -F':' '{print $2"."$1}' | sed 's/^\.//').iam.gserviceaccount.com"

# Set an easy name to invoke the sampler module
export AUTO_TOKENIZE_DIR="${PWD}"
alias sample_and_identify_pipeline='java -jar ${AUTO_TOKENIZE_DIR}/sampler-pipeline/target/sampler-pipeline-bundled-0.1-SNAPSHOT.jar'

alias tokenize_pipeline='java -jar ${AUTO_TOKENIZE_DIR}/encrypting-pipeline/target/encrypting-pipeline-bundled-0.1-SNAPSHOT.jar'
