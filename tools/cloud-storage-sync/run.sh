#!/bin/bash

set -e

print_usage() {
  printf '%s\n' 'Usage: ./run.sh SOURCE_DIR TARGET_DIR FREQUENCY [PORT]' 'FREQUENCY: forever/once' 'If PORT is specified, a web server will be run on the given port'
}

if [ $# -lt 3 ]; then
    print_usage
    exit 1
fi

SOURCE_DIR=$1
TARGET_DIR=$2
FREQUENCY=$3
PORT=$4

PID_FILE=server.pid

if [ "$PORT" != "" ]; then
    echo "Starting server on port $PORT ..."
    gunicorn server:app --bind=0.0.0.0:"$PORT" --workers=1 -p $PID_FILE --daemon
fi

python3 sync.py "$SOURCE_DIR" "$TARGET_DIR" "$FREQUENCY"

if [ "$PORT" != "" ]; then
    echo "Killing server ..."
    kill "$(cat "$PID_FILE")"
fi
