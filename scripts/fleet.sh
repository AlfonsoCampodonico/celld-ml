#!/usr/bin/env bash
# Run a local fleet inside the `mirage` Lima VM: one bucket and up to three
# nodes, each its own Solo5 hvt unikernel on a tap device.
#
#   scripts/fleet.sh up [nodes] [node args...]   bucket + n1..n<nodes> (default 3)
#   scripts/fleet.sh node <i> [node args...]     (re)start node n<i>
#   scripts/fleet.sh kill <i>                    SIGKILL node n<i>
#   scripts/fleet.sh down                        stop everything
#   scripts/fleet.sh logs <bucket|n1|n2|n3>
#
# Logs and the bucket's disk image live in /tmp/celld-ml in the VM.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
run=/tmp/celld-ml
hvt=$HOME/.opam/mirage/bin/solo5-hvt
mkdir -p "$run"

ip link show br0 >/dev/null 2>&1 || bash "$root/scripts/net.sh"

boot() { # name tap image args...
  local name=$1 tap=$2 image=$3; shift 3
  sg kvm -c "exec $hvt --mem=512 --net:service=$tap $* " </dev/null >"$run/$name.log" 2>&1 &
  echo $! >"$run/$name.pid"
}

wait_for() { # name pattern
  for _ in $(seq 1 100); do grep -q "$2" "$run/$1.log" 2>/dev/null && return 0; sleep 0.1; done
  echo "$1 did not report '$2':"; tail -20 "$run/$1.log"; return 1
}

start_bucket() {
  [ -f "$run/bucket.img" ] || truncate -s 256M "$run/bucket.img"
  boot bucket tap0 bucket "--block:storage=$run/bucket.img" "$root/unikernels/bucket/dist/bucket.hvt" \
    --ipv4=10.0.0.2/24 --ipv4-gateway=10.0.0.1 "${BUCKET_ARGS:-}"
  wait_for bucket "listening on port"
}

start_node() { # i args...
  local i=$1; shift
  stop_one "n$i"
  # The follower log survives restarts; `up` starts every node with a blank one.
  [ -f "$run/n$i.img" ] || truncate -s "${LOG_SIZE:-64M}" "$run/n$i.img"
  boot "n$i" "tap$i" node "--block:log=$run/n$i.img" "$root/unikernels/node/dist/node.hvt" \
    "--ipv4=10.0.0.1$i/24" --ipv4-gateway=10.0.0.1 "--node-id=n$i" "--advertise=10.0.0.1$i:8081" \
    --bucket=10.0.0.2:9000 "$@"
  wait_for "n$i" "celld-ml node"
}

stop_one() {
  # The pid file holds sg's pid; the hvt tender is its child (sg execs bash -c which execs hvt).
  if [ -f "$run/$1.pid" ]; then
    pkill -KILL -P "$(cat "$run/$1.pid")" 2>/dev/null || true
    kill -KILL "$(cat "$run/$1.pid")" 2>/dev/null || true
    rm -f "$run/$1.pid"
  fi
}

case ${1:-} in
  up)
    shift; n=${1:-3}; shift || true
    "$0" down
    rm -f "$run/bucket.img" "$run"/n*.img
    start_bucket
    for i in $(seq 1 "$n"); do start_node "$i" "$@"; done
    ;;
  node) shift; i=$1; shift; start_node "$i" "$@" ;;
  kill) stop_one "n$2" ;;
  down)
    for p in "$run"/*.pid; do [ -e "$p" ] && stop_one "$(basename "$p" .pid)"; done
    pkill -x solo5-hvt 2>/dev/null || true
    ;;
  logs) cat "$run/$2.log" ;;
  *) sed -n 2,12p "$0"; exit 1 ;;
esac
