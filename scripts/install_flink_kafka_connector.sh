#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIB_DIR="${LIB_DIR:-${PROJECT_ROOT}/lib}"
CONNECTOR_VERSION="${CONNECTOR_VERSION:-4.0.0-2.0}"
ARTIFACT_ID="${ARTIFACT_ID:-flink-sql-connector-kafka}"
MAVEN_REPO="${MAVEN_REPO:-https://repo.maven.apache.org/maven2}"
JAR_NAME="${ARTIFACT_ID}-${CONNECTOR_VERSION}.jar"
JAR_PATH="${LIB_DIR}/${JAR_NAME}"
JAR_URL="${MAVEN_REPO}/org/apache/flink/${ARTIFACT_ID}/${CONNECTOR_VERSION}/${JAR_NAME}"

mkdir -p "${LIB_DIR}"

if [[ -f "${JAR_PATH}" ]]; then
  printf 'Flink Kafka connector already exists: %s\n' "${JAR_PATH}"
  exit 0
fi

printf 'Downloading %s\n' "${JAR_URL}"
printf 'Saving to %s\n' "${JAR_PATH}"

if command -v curl >/dev/null 2>&1; then
  curl -fL "${JAR_URL}" -o "${JAR_PATH}"
elif command -v wget >/dev/null 2>&1; then
  wget -O "${JAR_PATH}" "${JAR_URL}"
else
  printf 'Neither curl nor wget is available. Please download the JAR manually:\n%s\n' "${JAR_URL}" >&2
  exit 1
fi

printf 'Done. Use it with:\n'
printf '  python flink_jobs/order_stream_job.py --source kafka --kafka-connector-jar %s\n' "${JAR_PATH}"
