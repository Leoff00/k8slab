#!/usr/bin/env sh
set -eu

NAMESPACE="${NAMESPACE:-labs}"
SERVICE="${SERVICE:-rabbitmq}"
LOCAL_PORT="${LOCAL_PORT:-15672}"
RABBITMQ_USER="${RABBITMQ_USER:-lab}"
RABBITMQ_PASS="${RABBITMQ_PASS:-lab}"
EXCHANGE_NAME="${EXCHANGE_NAME:-lab.events}"
ROUTING_KEY="${ROUTING_KEY:-labs.worker}"
PF_LOG="${PF_LOG:-/tmp/simple-worker-rabbit-port-forward.log}"

if [ "$#" -eq 0 ]; then
  set -- "hello from publish-rabbit.sh"
fi

cleanup() {
  if [ "${PORT_FORWARD_PID:-}" != "" ]; then
    kill "$PORT_FORWARD_PID" 2>/dev/null || true
    wait "$PORT_FORWARD_PID" 2>/dev/null || true
  fi
}

json_escape() {
  printf '%s' "$1" | sed ':a;N;$!ba;s/\n/\\n/g;s/\\/\\\\/g;s/"/\\"/g'
}

trap cleanup EXIT INT TERM

kubectl -n "$NAMESPACE" rollout status statefulset/rabbitmq --timeout=90s >/dev/null
kubectl -n "$NAMESPACE" port-forward "svc/$SERVICE" "${LOCAL_PORT}:15672" >"$PF_LOG" 2>&1 &
PORT_FORWARD_PID=$!

attempt=0
until curl -fsS -u "$RABBITMQ_USER:$RABBITMQ_PASS" "http://127.0.0.1:${LOCAL_PORT}/api/overview" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 20 ]; then
    echo "RabbitMQ management API nao ficou pronta. Veja $PF_LOG." >&2
    exit 1
  fi
  sleep 1
done

for message in "$@"; do
  escaped_message="$(json_escape "$message")"
  escaped_routing_key="$(json_escape "$ROUTING_KEY")"
  
  curl -fsS \
    -u "$RABBITMQ_USER:$RABBITMQ_PASS" \
    -H "content-type: application/json" \
    -X POST \
    "http://127.0.0.1:${LOCAL_PORT}/api/exchanges/%2F/${EXCHANGE_NAME}/publish" \
    --data "{\"properties\":{},\"routing_key\":\"${escaped_routing_key}\",\"payload\":\"${escaped_message}\",\"payload_encoding\":\"string\"}" \
    >/dev/null

  printf 'published routing_key=%s message=%s\n' "$ROUTING_KEY" "$message"
done
