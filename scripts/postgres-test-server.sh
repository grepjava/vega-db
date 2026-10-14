#!/usr/bin/env bash
# The server VegaPostgres's integration tests run against: PostgreSQL 17 in a
# Docker container, on 127.0.0.1:46432, with user `vega`, password
# vega-secret and database `vega`. A password set this way is stored as a
# SCRAM-SHA-256 verifier and asked for over TCP, so the tests authenticate
# with SCRAM. The user is a superuser, which the tests that create roles and
# end other sessions need. No TLS: the tests that require it expect a refusal.
#
# The server listens on the same port inside the container as outside it, so
# its unix socket is named for that port too. Works with Docker Desktop.
#
#   scripts/postgres-test-server.sh up     # start it, and print the variables
#   scripts/postgres-test-server.sh down   # remove it
#
# Where the engine shares this machine's filesystem (Docker on Linux, not
# Docker Desktop), the server also keeps its unix socket in VEGA_POSTGRES_DIR
# (/tmp/vega-postgres by default) for VEGA_POSTGRES_SOCKET.
# VEGA_POSTGRES_IMAGE picks the image: postgres:17 by default.
set -euo pipefail

IMAGE=${VEGA_POSTGRES_IMAGE:-postgres:17}
PORT=46432
USER_NAME=vega
PASS=vega-secret
DATABASE=vega
DIR=${VEGA_POSTGRES_DIR:-/tmp/vega-postgres}
NAME=vega-postgres

wait_for() {
    local what=$1 check=$2
    for _ in $(seq 1 120); do
        if eval "$check" >/dev/null 2>&1; then return 0; fi
        sleep 0.5
    done
    echo "timed out waiting for $what" >&2
    docker logs --tail 20 "$NAME" >&2 || true
    exit 1
}

down() {
    docker rm -f "$NAME" >/dev/null 2>&1 || true
}

up() {
    down
    local socket=()
    if ! docker info --format '{{.OperatingSystem}}' | grep -q 'Docker Desktop'; then
        mkdir -p "$DIR"
        chmod 777 "$DIR"
        socket=(-v "$DIR:/var/run/postgresql")
    fi
    docker run -d --name "$NAME" -p "127.0.0.1:$PORT:$PORT" "${socket[@]}" \
        -e PGPORT="$PORT" -e POSTGRES_USER="$USER_NAME" -e POSTGRES_PASSWORD="$PASS" \
        -e POSTGRES_DB="$DATABASE" "$IMAGE" >/dev/null

    # Over TCP and with the password: the server the image starts first, to
    # initialise the database, listens on its socket only, so a server that
    # answers here is the one that stays.
    wait_for "the server" \
        "docker exec -e PGPASSWORD=$PASS $NAME psql -h 127.0.0.1 -p $PORT -U $USER_NAME -d $DATABASE -tAc 'select 1' | grep -q 1"

    echo "export VEGA_POSTGRES=127.0.0.1:$PORT:$USER_NAME:$PASS:$DATABASE"
    if [ ${#socket[@]} -gt 0 ]; then echo "export VEGA_POSTGRES_SOCKET=$DIR"; fi
}

case "${1:-}" in
    up) up ;;
    down) down ;;
    *) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
