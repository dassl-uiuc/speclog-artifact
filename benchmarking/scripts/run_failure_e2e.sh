#!/bin/bash

source ./common.sh

user=$(whoami)

runtime_secs=60
computation_time=2000
num_shards=2

modify_batching_intervals 1ms

cleanup_clients
cleanup_servers
clear_server_logs
clear_client_logs

start_order_nodes
start_discovery
start_data_nodes $num_shards

sleep 1
start_e2e_clients ${client_nodes[0]} $computation_time $runtime_secs 0 16 $benchmark_dir/logs/

echo "wait 30s before killing 1 shard"
sleep 30s

ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $user@${data_pri[0]} "sudo bash -s" < ./kill_all_goreman.sh
ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $user@${data_sec[0]} "sudo bash -s" < ./kill_all_goreman.sh

echo "Shard 0 killed, waiting for clients to finish"

wait 

cleanup_clients
cleanup_servers
collect_logs $num_shards

# move logs to a different folder
mkdir -p "$benchmark_dir/results/e2e_${computation_time}"
mv $benchmark_dir/logs/* "$benchmark_dir/results/e2e_${computation_time}"

python3 ./draw_failure_e2e.py