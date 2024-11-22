#!/bin/bash

source ./common.sh

# two shards involved in this expt
num_shards=2

# when does shard 1 join?
shard_join_time=15

# parameters
runtime_secs=60
computation_time=(1000)

cleanup_clients
cleanup_servers
clear_server_logs
clear_client_logs

start_order_nodes
start_discovery

# start shard 0 with rid 0 and rid 1
start_specific_shard 0

# sleep for a bit befor starting clients
sleep 5

# start clients
start_reconfig_clients ${client_nodes[0]} $computation_time $runtime_secs 0 5 $benchmark_dir/logs/ 1
start_reconfig_clients ${client_nodes[1]} $computation_time $runtime_secs 1 5 $benchmark_dir/logs/ 0
echo "Waiting for clients to terminate"

# sleep for a bit before starting shard 1
sleep $shard_join_time

new_runtime_secs=$(($runtime_secs - $shard_join_time))
# start shard 1 with rid 2 and rid 3
start_specific_shard 1
start_reconfig_clients ${client_nodes[2]} $computation_time $new_runtime_secs 2 5 $benchmark_dir/logs/ 0
start_reconfig_clients ${client_nodes[3]} $computation_time $new_runtime_secs 3 5 $benchmark_dir/logs/ 0

wait 

cleanup_clients
cleanup_servers

collect_logs $num_shards

# move logs to a different folder
mkdir -p "$benchmark_dir/results/reconfig_${computation_time}"
mv $benchmark_dir/logs/* "$benchmark_dir/results/reconfig_${computation_time}"

