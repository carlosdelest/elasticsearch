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