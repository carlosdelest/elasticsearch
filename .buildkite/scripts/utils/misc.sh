#!/bin/bash
set -e

retry() {
  local waitInS="$1" # First argument
  local retries="$2" # Second argument
  local command="$3" # Third argument
  # Run the command, and save the exit code
  exit_code=0
  $command || exit_code=$?
  # If the exit code is non-zero (i.e. command failed), and we have not
  # reached the maximum number of retries, run the command again
  if [[ $exit_code -ne 0 && $retries -gt 0 ]]; then
    sleep $waitInS
    echo "Command not succesful"

    retry $waitInS $(($retries - 1)) "$command"
  else
    # Return the exit code from the command
    return $exit_code
  fi
}

vault_with_retries() {
  retry 5 5 "vault $*"
}

# We read data from vault early to avoid vault token for ci job beeing expired later in the job
ENCRYPTION_KEY=$(vault_with_retries read -field private-key secret/ci/elastic-elasticsearch-serverless/ess-delivery-ci-encryption)

encrypt () {
  local message="$1"
  local keyFile="encrypt.pem"
  echo "$ENCRYPTION_KEY" > $keyFile
  retValue=$(echo $message | openssl pkeyutl -encrypt -inkey $keyFile | openssl base64 -e)
  echo "$retValue"
}

decrypt () {
  local message="$1"
  local keyFile="encrypt.pem"
  echo "$ENCRYPTION_KEY" > $keyFile
  retValue=$(echo $message | openssl pkeyutl -decrypt -inkey $keyFile | openssl base64 -d)
  echo "$retValue"
}
