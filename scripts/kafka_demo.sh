#!/usr/bin/env bash
set -euo pipefail

TOPIC="${TOPIC:-ecommerce-orders}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-127.0.0.1:9092}"
PARTITIONS="${PARTITIONS:-1}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"
KAFKA_CONFIG="${KAFKA_CONFIG:-}"
MAX_MESSAGES="${MAX_MESSAGES:-5}"

usage() {
  cat <<'EOF'
Usage:
  scripts/kafka_demo.sh info
  scripts/kafka_demo.sh format
  scripts/kafka_demo.sh start
  scripts/kafka_demo.sh topic
  scripts/kafka_demo.sh status
  scripts/kafka_demo.sh consume

Environment:
  KAFKA_HOME=/path/to/kafka        Optional when kafka-*.sh is not on PATH.
  KAFKA_CONFIG=/path/server.properties
  BOOTSTRAP_SERVERS=127.0.0.1:9092
  TOPIC=ecommerce-orders
  PARTITIONS=1
  REPLICATION_FACTOR=1
  MAX_MESSAGES=5
EOF
}

kafka_cmd() {
  local name="$1"
  local bare_name="${name%.sh}"
  if [[ -n "${KAFKA_HOME:-}" && -x "${KAFKA_HOME}/bin/${name}" ]]; then
    printf '%s\n' "${KAFKA_HOME}/bin/${name}"
    return 0
  fi
  if [[ -n "${KAFKA_HOME:-}" && -x "${KAFKA_HOME}/bin/${bare_name}" ]]; then
    printf '%s\n' "${KAFKA_HOME}/bin/${bare_name}"
    return 0
  fi
  if [[ -n "${KAFKA_HOME:-}" && -x "${KAFKA_HOME}/libexec/bin/${name}" ]]; then
    printf '%s\n' "${KAFKA_HOME}/libexec/bin/${name}"
    return 0
  fi
  if [[ -n "${KAFKA_HOME:-}" && -x "${KAFKA_HOME}/libexec/bin/${bare_name}" ]]; then
    printf '%s\n' "${KAFKA_HOME}/libexec/bin/${bare_name}"
    return 0
  fi
  if command -v "${name}" >/dev/null 2>&1; then
    command -v "${name}"
    return 0
  fi
  if command -v "${bare_name}" >/dev/null 2>&1; then
    command -v "${bare_name}"
    return 0
  fi
  printf 'Could not find %s. Set KAFKA_HOME=/path/to/kafka or add Kafka bin to PATH.\n' "${name}" >&2
  return 1
}

default_config() {
  if [[ -n "${KAFKA_CONFIG}" ]]; then
    printf '%s\n' "${KAFKA_CONFIG}"
    return 0
  fi
  if [[ -n "${KAFKA_HOME:-}" ]]; then
    if [[ -f "${KAFKA_HOME}/config/server.properties" ]]; then
      printf '%s\n' "${KAFKA_HOME}/config/server.properties"
      return 0
    fi
    if [[ -f "${KAFKA_HOME}/config/kraft/server.properties" ]]; then
      printf '%s\n' "${KAFKA_HOME}/config/kraft/server.properties"
      return 0
    fi
    if [[ -f "${KAFKA_HOME}/libexec/config/server.properties" ]]; then
      printf '%s\n' "${KAFKA_HOME}/libexec/config/server.properties"
      return 0
    fi
    if [[ -f "${KAFKA_HOME}/libexec/config/kraft/server.properties" ]]; then
      printf '%s\n' "${KAFKA_HOME}/libexec/config/kraft/server.properties"
      return 0
    fi
  fi
  printf 'Could not find Kafka server.properties. Set KAFKA_CONFIG=/path/to/server.properties.\n' >&2
  return 1
}

format_storage() {
  local storage_cmd
  local config
  local cluster_id
  storage_cmd="$(kafka_cmd kafka-storage.sh)"
  config="$(default_config)"
  cluster_id="$("${storage_cmd}" random-uuid)"

  printf 'Formatting Kafka storage with config: %s\n' "${config}"
  if "${storage_cmd}" format --standalone --ignore-formatted -t "${cluster_id}" -c "${config}"; then
    return 0
  fi

  printf 'Retrying Kafka storage format without --standalone for older Kafka layouts.\n'
  "${storage_cmd}" format --ignore-formatted -t "${cluster_id}" -c "${config}"
}

start_kafka() {
  local server_cmd
  local config
  server_cmd="$(kafka_cmd kafka-server-start.sh)"
  config="$(default_config)"
  printf 'Starting Kafka with config: %s\n' "${config}"
  printf 'Leave this terminal running. Stop with Ctrl+C.\n'
  exec "${server_cmd}" "${config}"
}

create_topic() {
  local topics_cmd
  topics_cmd="$(kafka_cmd kafka-topics.sh)"
  "${topics_cmd}" \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --create \
    --if-not-exists \
    --topic "${TOPIC}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}"

  "${topics_cmd}" --bootstrap-server "${BOOTSTRAP_SERVERS}" --describe --topic "${TOPIC}"
}

status() {
  local broker_api_cmd
  broker_api_cmd="$(kafka_cmd kafka-broker-api-versions.sh)"
  "${broker_api_cmd}" --bootstrap-server "${BOOTSTRAP_SERVERS}" >/dev/null
  printf 'Kafka broker is reachable at %s\n' "${BOOTSTRAP_SERVERS}"
}

consume() {
  local consumer_cmd
  consumer_cmd="$(kafka_cmd kafka-console-consumer.sh)"
  "${consumer_cmd}" \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --topic "${TOPIC}" \
    --from-beginning \
    --max-messages "${MAX_MESSAGES}"
}

info() {
  printf 'Kafka command discovery:\n'
  kafka_cmd kafka-storage.sh
  kafka_cmd kafka-server-start.sh
  kafka_cmd kafka-topics.sh
  kafka_cmd kafka-broker-api-versions.sh
  kafka_cmd kafka-console-consumer.sh
  printf '\nKafka config: %s\n' "$(default_config)"
  printf 'Bootstrap servers: %s\n' "${BOOTSTRAP_SERVERS}"
  printf 'Topic: %s\n' "${TOPIC}"
}

case "${1:-}" in
  info)
    info
    ;;
  format)
    format_storage
    ;;
  start)
    start_kafka
    ;;
  topic)
    create_topic
    ;;
  status)
    status
    ;;
  consume)
    consume
    ;;
  -h|--help|help|"")
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
