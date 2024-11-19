#!/bin/bash

source ./common.sh

# parameters
runtime_secs=20
# computation_time=(100 200 500 800 1000 1200)
computation_time=(1000)
num_shards=1

for computation_time in "${computation_time[@]}";
do 
    cleanup_clients
    cleanup_servers
    clear_server_logs
    clear_client_logs

    start_order_nodes
    start_discovery
    start_data_nodes $num_shards

    sleep 1
    start_e2e_clients ${client_nodes[0]} $computation_time $runtime_secs 0 10 $benchmark_dir/logs/
    start_e2e_clients ${client_nodes[1]} $computation_time $runtime_secs 1 10 $benchmark_dir/logs/
    echo "Waiting for clients to terminate"

    wait 

    cleanup_clients
    cleanup_servers
    collect_logs $num_shards

    # move logs to a different folder
    mkdir -p "$benchmark_dir/results/e2e_${computation_time}"
    mv $benchmark_dir/logs/* "$benchmark_dir/results/e2e_${computation_time}"
done

