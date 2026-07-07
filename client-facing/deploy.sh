#!/bin/bash
set -e

# Location-independent: works whether invoked as ./deploy.sh from inside
# client-facing/, as client-facing/deploy.sh from the repo root, or via an
# absolute path from anywhere. Paths below are built from these two anchors
# rather than assuming a particular cwd.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BUCKET="state-level-cf-frontend"
ECR="065148239427.dkr.ecr.us-east-2.amazonaws.com/cf-api:latest"
CLUSTER="state-level-cf"
SERVICE="campaign-finance-api"
REGION="us-east-2"

usage() {
  echo "Usage: ./deploy.sh [frontend|api|data|all]"
  exit 1
}

restart_api() {
  echo "Restarting ECS service to pick up new DB..."
  aws ecs update-service \
    --cluster $CLUSTER \
    --service $SERVICE \
    --force-new-deployment \
    --region $REGION \
    --no-cli-pager
  echo "ECS restart triggered. New container will pull DB from S3 on startup."
}

deploy_data() {
  echo "Pushing DB to S3..."
  # Pipeline code lives at the project root, one level up from client-facing/
  python3 "$PROJECT_ROOT/src/main.py" push db
  restart_api
}

deploy_frontend() {
  echo "Deploying frontend..."
  cd "$SCRIPT_DIR"
  # Swap to prod config
  sed -i '' 's|config/config.dev.js|config/config.prod.js|g' frontend/index.html frontend/downloads.html
  # Upload
  aws s3 cp frontend/index.html        s3://$BUCKET/index.html
  aws s3 cp frontend/styles.css        s3://$BUCKET/styles.css
  aws s3 cp frontend/downloads.html    s3://$BUCKET/downloads.html
  aws s3 cp frontend/data-quality.html s3://$BUCKET/data-quality.html
  aws s3 cp frontend/config/config.prod.js s3://$BUCKET/config/config.prod.js
  # Swap back to dev
  sed -i '' 's|config/config.prod.js|config/config.dev.js|g' frontend/index.html frontend/downloads.html
  echo "Frontend deployed."
}

deploy_api() {
  echo "Building and pushing API image..."
  cd "$SCRIPT_DIR/api"
  docker buildx build --platform linux/amd64 -t $ECR --push .
  echo "Forcing ECS redeployment..."
  aws ecs update-service \
    --cluster $CLUSTER \
    --service $SERVICE \
    --force-new-deployment \
    --region $REGION \
    --no-cli-pager
  echo "API deployed. New task will start shortly."
}

case "${1:-all}" in
  frontend) deploy_frontend ;;
  api)      deploy_api ;;
  data)     deploy_data ;;
  all)      deploy_frontend && deploy_api ;;
  *)        usage ;;
esac
