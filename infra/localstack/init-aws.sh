#!/usr/bin/env bash
# Runs inside the LocalStack container on startup (mounted to
# /etc/localstack/init/ready.d/ -- see docker-compose.yml) to create the
# DynamoDB tables and SQS queues AtlasFlow needs. This is LocalStack-only
# bootstrapping for local development/demo purposes; a real deployment
# would create this same infrastructure via Terraform/CDK/CloudFormation
# instead (see docs/architecture.md).
set -euo pipefail

echo "Creating AtlasFlow DynamoDB tables..."

awslocal dynamodb create-table \
    --table-name atlasflow-executions \
    --attribute-definitions AttributeName=executionId,AttributeType=S \
    --key-schema AttributeName=executionId,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

awslocal dynamodb create-table \
    --table-name atlasflow-tasks \
    --attribute-definitions \
        AttributeName=executionId,AttributeType=S \
        AttributeName=taskId,AttributeType=S \
    --key-schema \
        AttributeName=executionId,KeyType=HASH \
        AttributeName=taskId,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST

awslocal dynamodb create-table \
    --table-name atlasflow-idempotency \
    --attribute-definitions AttributeName=idempotencyKey,AttributeType=S \
    --key-schema AttributeName=idempotencyKey,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

echo "Creating AtlasFlow SQS queues..."

awslocal sqs create-queue --queue-name atlasflow-tasks-dlq

DLQ_ARN=$(awslocal sqs get-queue-attributes \
    --queue-url "$(awslocal sqs get-queue-url --queue-name atlasflow-tasks-dlq --query QueueUrl --output text)" \
    --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

awslocal sqs create-queue \
    --queue-name atlasflow-tasks \
    --attributes "{
        \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"10\\\"}\",
        \"VisibilityTimeout\": \"60\"
    }"

echo "AtlasFlow LocalStack bootstrap complete."
echo "  Tables: atlasflow-executions, atlasflow-tasks, atlasflow-idempotency"
echo "  Queues: atlasflow-tasks, atlasflow-tasks-dlq"
