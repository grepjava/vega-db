#!/usr/bin/env bash
# The servers VegaRedis's integration tests run against, all in one Docker
# container with their ports published on 127.0.0.1:
#
#   - one server on 46379, password vega-secret, and an ACL user `app`
#     (password app-secret) allowed only keys under `app:`;
#   - a cluster of three masters and three replicas on 46001-46006;
#   - a master on 46380, its replica on 46381, and three sentinels on
#     46390-46392 watching it as `vega`.
#
# One container, so every server shares one network: the addresses the
# cluster and the sentinels hand out (127.0.0.1 and these ports) work inside
# it and, published, outside it too. Works with Docker Desktop.
#
#   scripts/redis-test-servers.sh up     # start them, and print the variables
#   scripts/redis-test-servers.sh down   # remove them
#
# Where the engine shares this machine's filesystem (Docker on Linux, not
# Docker Desktop), the single server also listens on a unix socket in
# VEGA_REDIS_DIR (/tmp/vega-redis by default) for VEGA_REDIS_SOCKET.
# VEGA_REDIS_IMAGE picks the image: valkey/valkey:8.1 by default; redis:7.4
# works too.
set -euo pipefail

IMAGE=${VEGA_REDIS_IMAGE:-valkey/valkey:8.1}
PASS=vega-secret
DIR=${VEGA_REDIS_DIR:-/tmp/vega-redis}
NAME=vega-redis
case "$IMAGE" in redis*) SERVER=redis-server CLI=redis-cli ;; *) SERVER=valkey-server CLI=valkey-cli ;; esac

in_container() { docker exec "$NAME" "$@"; }

serve() {
    in_container "$SERVER" --daemonize yes --save '' --appendonly no --dir /data \
        --requirepass "$PASS" --masterauth "$PASS" "$@" >/dev/null
}

cli() {
    local port=$1
    shift
    in_container "$CLI" -p "$port" -a "$PASS" --no-auth-warning "$@"
}

wait_for() {
    local what=$1 check=$2
    for _ in $(seq 1 60); do
        if eval "$check" >/dev/null 2>&1; then return 0; fi
        sleep 0.5
    done
    echo "timed out waiting for $what" >&2
    exit 1
}

down() {
    docker rm -f "$NAME" >/dev/null 2>&1 || true
}

up() {
    down
    local ports=() socket=()
    for port in 46379 46380 46381 46390 46391 46392 46001 46002 46003 46004 46005 46006; do
        ports+=(-p "127.0.0.1:$port:$port")
    done
    if ! docker info --format '{{.OperatingSystem}}' | grep -q 'Docker Desktop'; then
        mkdir -p "$DIR"
        chmod 777 "$DIR"
        socket=(-v "$DIR:/sock")
    fi
    docker run -d --name "$NAME" "${ports[@]}" "${socket[@]}" --entrypoint sleep "$IMAGE" infinity >/dev/null

    # One server.
    local unix=()
    if [ ${#socket[@]} -gt 0 ]; then unix=(--unixsocket /sock/redis.sock --unixsocketperm 777); fi
    serve --port 46379 "${unix[@]}" --user app on '>app-secret' '~app:*' '&*' '+@all'
    wait_for "the server" "cli 46379 PING | grep -q PONG"

    # The cluster.
    local nodes=()
    for port in 46001 46002 46003 46004 46005 46006; do
        serve --port "$port" --cluster-enabled yes --cluster-config-file "nodes-$port.conf" \
            --cluster-node-timeout 2000 --cluster-announce-ip 127.0.0.1
        nodes+=("127.0.0.1:$port")
    done
    for port in 46001 46002 46003 46004 46005 46006; do
        wait_for "cluster node $port" "cli $port PING | grep -q PONG"
    done
    in_container "$CLI" -a "$PASS" --no-auth-warning --cluster create "${nodes[@]}" \
        --cluster-replicas 1 --cluster-yes >/dev/null
    wait_for "the cluster" "cli 46001 CLUSTER INFO | grep -q cluster_state:ok"

    # Sentinel: a master, a replica, three sentinels.
    serve --port 46380
    serve --port 46381 --replicaof 127.0.0.1 46380
    wait_for "the replica" "cli 46381 INFO replication | grep -q master_link_status:up"
    for port in 46390 46391 46392; do
        in_container sh -c "printf '%s\n' 'port $port' 'daemonize yes' 'dir /data' \
            'sentinel monitor vega 127.0.0.1 46380 2' 'sentinel auth-pass vega $PASS' \
            'sentinel down-after-milliseconds vega 2000' 'sentinel failover-timeout vega 10000' \
            > /data/sentinel-$port.conf"
        in_container "$SERVER" "/data/sentinel-$port.conf" --sentinel
    done
    wait_for "the sentinels" \
        "in_container $CLI -p 46390 SENTINEL master vega | grep -A1 num-other-sentinels | tail -1 | grep -q 2"

    echo "export VEGA_REDIS=127.0.0.1:46379:$PASS"
    if [ ${#socket[@]} -gt 0 ]; then echo "export VEGA_REDIS_SOCKET=$DIR/redis.sock"; fi
    echo "export VEGA_REDIS_CLUSTER=127.0.0.1:46001:$PASS"
    echo "export VEGA_REDIS_SENTINEL=127.0.0.1:46390:vega:$PASS"
}

case "${1:-}" in
    up) up ;;
    down) down ;;
    *) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
