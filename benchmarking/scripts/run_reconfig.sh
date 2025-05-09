#!/bin/bash

source ./common.sh

# two shards involved in this expt
num_shards=2

# when does shard 1 join?
shard_join_time=15

# when does shard 1 leave
# shard 1 leaves 30 seconds after joining, uncomment code in data_server.go to achieve this. 
# also uncomment timeout code in client.go

# parameters
runtime_secs=60
computation_time=(800)

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

new_client_runtime_secs=$(($runtime_secs - $shard_join_time))
# start clients
start_reconfig_clients ${client_nodes[0]} $computation_time $runtime_secs $new_client_runtime_secs 10 $benchmark_dir/logs/ 1
echo "Waiting for clients to terminate"

# sleep for a bit before starting shard 1
sleep $shard_join_time

# start shard 1 with rid 2 and rid 3
start_specific_shard 1

wait 

cleanup_clients
cleanup_servers

collect_logs $num_shards

# move logs to a different folder
mkdir -p "$benchmark_dir/results/reconfig_${computation_time}"
mv $benchmark_dir/logs/* "$benchmark_dir/results/reconfig_${computation_time}"

