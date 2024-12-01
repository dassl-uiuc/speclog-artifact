#!/bin/bash

source ./common.sh

num_shards=(20)
for num_shard in "${num_shards[@]}"
do
    echo "Running emulation for $num_shard shards"
    cleanup_clients
    cleanup_servers
    clear_server_logs
    clear_client_logs

    start_order_nodes
    start_discovery
    start_data_nodes ${num_shard}

    sleep 140

    cleanup_clients
    cleanup_servers
    collect_logs ${num_shard}

    # move logs to a different folder
    mkdir -p "$benchmark_dir/results/emulation_$num_shard"
    mv $benchmark_dir/logs/* "$benchmark_dir/results/emulation_$num_shard"
done 



