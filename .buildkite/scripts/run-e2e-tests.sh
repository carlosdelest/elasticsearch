#!/bin/bash
set -euo pipefail

source "$BUILDKITE_DIR/scripts/utils/misc.sh"

vault_with_retries read -field private-key secret/ci/elastic-elasticsearch-serverless/ess-delivery-ci-encryption > key.pem
export ESS_PUBLIC_URL=$(buildkite-agent meta-data get ess-public-url)
export ESS_PROJECT_ID=$(buildkite-agent meta-data get ess-project-id)
export ESS_USERNAME=$(buildkite-agent meta-data get ess-username)
export ESS_PASSWORD=$(buildkite-agent meta-data get ess-password-encrypted | openssl base64 -d | openssl pkeyutl -decrypt -inkey key.pem)

checkEssAvailability() {
    curl -u $ESS_USERNAME:$ESS_PASSWORD "$ESS_PUBLIC_URL/_cluster/health" | jq -e 'select(.status == "green")'
}

# wait for a maximum of 20 minutes
retry 30 40 checkEssAvailability

.buildkite/scripts/run-gradle.sh :qa:e2e-test:javaRestTest
