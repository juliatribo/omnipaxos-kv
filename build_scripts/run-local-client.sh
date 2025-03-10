#!/bin/bash

clients_num=7
rust_log="info"

# Clean up child processes
interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT

# Clients' output is saved into logs dir
local_experiment_dir="./logs"
mkdir -p "${local_experiment_dir}"

# Run clients
for ((i = 1; i <= clients_num; i++)); do
    client_config_path="./client-${i}-config.toml"
    RUST_LOG=$rust_log CONFIG_FILE="$client_config_path"  cargo run --manifest-path="../Cargo.toml" --bin client --release &
done
wait