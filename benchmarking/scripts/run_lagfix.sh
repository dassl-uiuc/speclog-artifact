#!/bin/bash

source ./common.sh

# single shard involved in this expt
num_shards=1

# ensure that quota is fixed to 10 in the data server and order server before starting this experiment i.e. turn off quota change

# parameters
runtime_secs=30

cleanup_clients
cleanup_servers
clear_server_logs
clear_client_logs

start_order_nodes
start_discovery

# start shard 0 with rid 0 and rid 1
start_data_nodes $num_shards

# sleep for a bit befor starting clients
sleep 5

# start clients
start_lagfix_clients ${client_nodes[0]} $runtime_secs 0 3 $benchmark_dir/logs/
start_lagfix_clients ${client_nodes[1]} $runtime_secs 1 3 $benchmark_dir/logs/
echo "Waiting for clients to terminate"

wait 

cleanup_clients
cleanup_servers

collect_logs $num_shards

# move logs to a different folder
mkdir -p "$benchmark_dir/results/lagfix"
mv $benchmark_dir/logs/* "$benchmark_dir/results/lagfix"

