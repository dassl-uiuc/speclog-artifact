#!/bin/bash

source ./common.sh

num_shards=5

cleanup_clients
cleanup_servers
clear_server_logs
clear_client_logs

start_order_nodes
start_discovery
start_data_nodes $num_shards

sleep 120

cleanup_clients
cleanup_servers
collect_logs $num_shards

# move logs to a different folder
mkdir -p "$benchmark_dir/results/emulation"
mv $benchmark_dir/logs/* "$benchmark_dir/results/emulation"

