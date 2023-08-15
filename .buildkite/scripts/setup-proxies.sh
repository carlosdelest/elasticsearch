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

set -euo pipefail

source "$BUILDKITE_DIR/scripts/utils/eks.sh"

aws_auth
aws_get_cluster_credentials

echo '--- Expose global ingress nginx controller'

kubectl expose svc/global-ingress-nginx-user-ingress-controller --port=8443 --target-port=443 --name=project-api-dev-proxy --type=LoadBalancer -n elastic-system

echo '--- Apply ess loadbalancer configuration'

kubectl apply -f $BUILDKITE_DIR/steps/k8s/ess-proxy.yaml
