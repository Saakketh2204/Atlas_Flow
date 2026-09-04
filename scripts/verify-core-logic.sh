#!/usr/bin/env bash
# Compiles and runs AtlasFlow's core distributed-systems logic (retry
# policy, consistent hashing, workflow DAG scheduling, idempotency keys,
# lease expiration) using nothing but a JDK -- no Maven, no internet
# access, no Spring/AWS SDK/JUnit required.
#
# This works because src/main/java/com/atlasflow/core/** is deliberately
# kept free of any external dependency (see the class-level Javadoc on
# RetryPolicy for why). Everything else in this repo (the REST API, the
# AWS SDK integration, the Spring Boot wiring) does need Maven Central and
# is validated by the real JUnit/Testcontainers suite in CI instead --
# see .github/workflows/ci.yml.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "$BUILD_DIR"' EXIT

echo "Compiling core logic + domain model + offline verifier..."
javac -encoding UTF-8 -d "$BUILD_DIR" \
  $(find "$REPO_ROOT/src/main/java/com/atlasflow/core" "$REPO_ROOT/src/main/java/com/atlasflow/domain" -name "*.java") \
  "$REPO_ROOT/tools/offline-verify/OfflineCoreVerifier.java"

echo "Running..."
echo
java -Dfile.encoding=UTF-8 -cp "$BUILD_DIR" OfflineCoreVerifier
