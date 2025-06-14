#!/bin/bash

source ../../common.sh
pushd $benchmark_dir/scripts

# parameters
runtime_secs=60
computation_time=(100 500 1000 1500 2000 2500 3000 4000 5000 50000)
num_shards=2

for ct in "${computation_time[@]}";
do 
    cleanup_clients
    cleanup_servers
    clear_server_logs
    clear_client_logs

    start_order_nodes
    start_discovery
    start_data_nodes $num_shards

    sleep 1
    start_e2e_clients ${client_nodes[0]} $ct $runtime_secs 0 10 $benchmark_dir/logs/
    start_e2e_clients ${client_nodes[1]} $ct $runtime_secs 1 10 $benchmark_dir/logs/
    start_e2e_clients ${client_nodes[2]} $ct $runtime_secs 2 10 $benchmark_dir/logs/
    start_e2e_clients ${client_nodes[3]} $ct $runtime_secs 3 10 $benchmark_dir/logs/
    echo "Waiting for clients to terminate"

    wait 

    cleanup_clients
    cleanup_servers
    collect_logs $num_shards

    # move logs to a different folder
    mkdir -p "$results_dir/e2e/speclog/e2e_4shard/e2e_${ct}"
    mv $benchmark_dir/logs/* "$results_dir/e2e/speclog/e2e_4shard/e2e_${ct}"
done

popd

