#!/usr/bin/env bash
# One bridge, four taps: tap0 = bucket (10.0.0.2), tap1..3 = nodes (10.0.0.11..13).
# The VM itself is 10.0.0.1 on br0. Idempotent.
set -euo pipefail
sudo ip link add br0 type bridge 2>/dev/null || true
sudo ip addr add 10.0.0.1/24 dev br0 2>/dev/null || true
sudo ip link set br0 up
for i in 0 1 2 3; do
  sudo ip tuntap add "tap$i" mode tap user "$USER" 2>/dev/null || true
  sudo ip link set "tap$i" master br0
  sudo ip link set "tap$i" up
done
