#!/bin/bash

# This script is used to run a slow shard test. It will run two physical shard setup with one slow shard that reports infrequently 

# trigger slow shard flag in shard server and ordering layer

source ./common.sh

num_shards=2

# parameters
runtime_secs=60
computation_time=1200

cleanup_clients
cleanup_servers
clear_server_logs
clear_client_logs

start_order_nodes
start_discovery

# start shard 0 with rid 0 and rid 1
start_data_nodes $num_shards

# sleep for a bit before starting clients
sleep 1

# start clients
start_straggler_clients ${client_nodes[0]} $computation_time $runtime_secs 0 3 $benchmark_dir/logs/
start_straggler_clients ${client_nodes[1]} $computation_time $runtime_secs 1 3 $benchmark_dir/logs/
start_straggler_clients ${client_nodes[0]} $computation_time $runtime_secs 2 3 $benchmark_dir/logs/
start_straggler_clients ${client_nodes[1]} $computation_time $runtime_secs 3 3 $benchmark_dir/logs/

echo "Waiting for clients to terminate"

wait 

cleanup_clients
cleanup_servers

collect_logs $num_shards

# move logs to a different folder
mkdir -p "$benchmark_dir/results/slowshard"
mv $benchmark_dir/logs/* "$benchmark_dir/results/slowshard"