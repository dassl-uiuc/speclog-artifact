#!/bin/bash

source ./common.sh

# one shard involved in this expt
num_shards=1

## uncomment relavent code in data_server and order_server


# new client joins when?
new_client_join_time=30 
computation_time=(800)

# parameters
runtime_secs=60

cleanup_clients
cleanup_servers
clear_server_logs
clear_client_logs

start_order_nodes
start_discovery

# start shards
start_data_nodes $num_shards

# sleep for a bit befor starting clients
sleep 5

# start clients
start_qc_e2e_clients ${client_nodes[0]} $runtime_secs 5 $new_client_join_time $computation_time $benchmark_dir/logs/ 
echo "Waiting for clients to terminate"

wait 

cleanup_clients
cleanup_servers

collect_logs $num_shards

# move logs to a different folder
mkdir -p "$benchmark_dir/results/qc"
mv $benchmark_dir/logs/* "$benchmark_dir/results/qc"

