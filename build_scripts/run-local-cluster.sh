#!/bin/bash

usage="Usage: run-local-cluster.sh"
cluster_size=5
rust_log="info"

# Clean up child processes
interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT

# Servers' output is saved into bencharking directory
local_experiment_dir="../benchmarks/logs/local-run"
mkdir -p "${local_experiment_dir}"

# Run servers
for ((i = 1; i <= cluster_size; i++)); do
    config_path="./server-${i}-config.toml"
    RUST_LOG=$rust_log CONFIG_FILE="$config_path" cargo run --manifest-path="../Cargo.toml" --bin server &
done
wait

