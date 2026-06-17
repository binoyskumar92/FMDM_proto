#!/bin/bash
set -euo pipefail

SECRET_NAME="fmdm/hash-salt"
AWS_REGION="us-east-1"

create_secret_if_missing() {
  if aws secretsmanager describe-secret \
    --secret-id "$SECRET_NAME" \
    --region "$AWS_REGION" >/dev/null 2>&1; then
    echo "Secret already exists. Not regenerating salt."
  else
    echo "Secret does not exist. Generating salt once."

    SALT=$(python3 -c "import secrets; print(secrets.token_hex(32))")

    aws secretsmanager create-secret \
      --name "$SECRET_NAME" \
      --description "FMDM hash salt" \
      --secret-string "{\"FMDM_HASH_SALT\":\"$SALT\"}" \
      --region "$AWS_REGION" >/dev/null

    echo "Secret created in AWS Secrets Manager."
  fi
}

load_secret_to_env() {
  SECRET_JSON=$(aws secretsmanager get-secret-value \
    --secret-id "$SECRET_NAME" \
    --region "$AWS_REGION" \
    --query SecretString \
    --output text)

  export FMDM_HASH_SALT=$(python3 -c "
import json, sys
data = json.loads(sys.stdin.read())
print(data['FMDM_HASH_SALT'])
" <<< "$SECRET_JSON")

  echo "FMDM_HASH_SALT loaded into environment."
}

create_secret_if_missing
load_secret_to_env

echo "Current env value:"
echo "FMDM_HASH_SALT=$FMDM_HASH_SALT"