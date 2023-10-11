#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0 and the Server Side Public License, v 1; you may not use this file except
# in compliance with, at your election, the Elastic License 2.0 or the Server
# Side Public License, v 1.
#

set -euo pipefail
source "$BUILDKITE_DIR/scripts/utils/misc.sh"

scripts_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

set +x
data=$(vault_with_retries read -format=json aws-elastic-ci-prod/creds/elasticsearch-ci-s3)
export stateless_aws_s3_access_key=$(echo $data | jq -r .data.access_key)
export stateless_aws_s3_secret_key=$(echo $data | jq -r .data.secret_key)
export stateless_aws_s3_session_token=$(echo $data | jq -r .data.security_token)
export stateless_aws_s3_region=us-west-2
export stateless_aws_s3_bucket=elasticsearch-ci.us-west-2
export stateless_aws_s3_base_path=stateless-cas-linearizability
set -x

source $scripts_dir/run-gradle.sh statelessS3ThirdPartyTests
