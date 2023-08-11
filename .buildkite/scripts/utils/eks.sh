
# The used service account
export AWS_SERVICE_ACCOUNT_VAULT_PATH="aws-elastic-ci-prod/creds/ecdev-secret-engine-role"

# The AWS profile to use
export AWS_PROFILE="ecdev"

# The file where the AWS credentials are stored
export AWS_CREDS_FILE="/tmp/aws-creds.json"

aws_auth() {
  echo "--- Reading AWS service account from Vault..."
  vault read -format=json ${AWS_SERVICE_ACCOUNT_VAULT_PATH} ttl=12h > ${AWS_CREDS_FILE}
  export AWS_ACCESS_KEY_ID=$(cat "${AWS_CREDS_FILE}"| jq -r '.data.access_key')
  export AWS_SECRET_ACCESS_KEY=$(cat "${AWS_CREDS_FILE}"| jq -r '.data.secret_key')
  export AWS_SESSION_TOKEN=$(cat "${AWS_CREDS_FILE}"| jq -r '.data.security_token')

  echo "--- Create AWS profile..."
  aws configure set aws_access_key_id ${AWS_ACCESS_KEY_ID} --profile ${AWS_PROFILE}
  aws configure set aws_secret_access_key ${AWS_SECRET_ACCESS_KEY} --profile ${AWS_PROFILE}
  aws configure set aws_session_token ${AWS_SESSION_TOKEN} --profile ${AWS_PROFILE}
}

aws_get_cluster_credentials() {
  aws_auth
  echo "--- Getting AWS EKS cluster credentials"
  eksctl utils write-kubeconfig \
    --profile=${AWS_PROFILE} \
    --region=${AWS_REGION} \
    --cluster=${AWS_CLUSTER_NAME}
}