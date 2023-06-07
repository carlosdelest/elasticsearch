#!/usr/bin/env bash

# ELASTICSEARCH CONFIDENTIAL
# __________________
#
#  Copyright Elasticsearch B.V. All rights reserved.
#
# NOTICE:  All information contained herein is, and remains
# the property of Elasticsearch B.V. and its suppliers, if any.
# The intellectual and technical concepts contained herein
# are proprietary to Elasticsearch B.V. and its suppliers and
# may be covered by U.S. and Foreign Patents, patents in
# process, and are protected by trade secret or copyright
# law.  Dissemination of this information or reproduction of
# this material is strictly forbidden unless prior written
# permission is obtained from Elasticsearch B.V.


FILE_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

gcloud_auth() {
    local gcloud_project=$1; shift
    local gcloud_service_account_vault_path=$1; shift
    GCLOUD_HOME=$HOME/.gcp
    GCLOUD_KEY_FILE=$GCLOUD_HOME/osServiceAccount.json
    echo "--- Reading GCP service account from vault..."
    SERVICE_ACCOUNT=$(vault read --field=data --format=json $gcloud_service_account_vault_path)
    if [ -z "$SERVICE_ACCOUNT" ]; then
        echo "Failed to get GCP service_account from vault."
        exit 1
    fi

    echo "--- Creating GCP service account key file..."
    mkdir -p $GCLOUD_HOME
    echo "$SERVICE_ACCOUNT" > $GCLOUD_KEY_FILE
    gcloud config set project $gcloud_project --quiet

    echo "--- Authenticating as GCP service account..."
    gcloud auth login --cred-file=$GCLOUD_KEY_FILE --quiet
}

gcloud_gke_get_cluster_credentials() {
    local gcloud_project=$1; shift
    local gke_cluster_name=$1; shift
    local gcloud_region=$1; shift
    echo "--- Getting GKE cluster credentials..."
    gcloud container clusters --project $gcloud_project get-credentials $gke_cluster_name --region $gcloud_region --quiet
}

gke_get_cluster_credentials() {
    local gcloud_service_account_vault_path=$1; shift
    local gcloud_project=$1; shift
    local gke_cluster_name=$1; shift
    local gcloud_region=$1; shift
    gcloud_auth $gcloud_project $gcloud_service_account_vault_path
    gcloud_gke_get_cluster_credentials $gcloud_project $gke_cluster_name $gcloud_region
}
