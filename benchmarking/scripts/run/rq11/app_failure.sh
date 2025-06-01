#!/bin/bash

source ../../common.sh
pushd $benchmark_dir/scripts

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
start_transaction_analysis_failure_clients ${client_nodes[0]} $computation_time $runtime_secs 0 16 $benchmark_dir/logs/ 100000

echo "wait 30s before killing 1 shard"
sleep 30s

ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_pri[0]} "sudo bash -s" < ./kill_all_goreman.sh
ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_sec[0]} "sudo bash -s" < ./kill_all_goreman.sh

echo "Shard 0 killed, waiting for clients to finish"

wait 

cleanup_clients
cleanup_servers
collect_logs $num_shards

# move logs to a different folder
mkdir -p "$results_dir/app_failure/e2e_${computation_time}"
mv $benchmark_dir/logs/* "$results_dir/app_failure/e2e_${computation_time}"

popd