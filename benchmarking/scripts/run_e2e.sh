#!/bin/bash

source ./common.sh

# parameters
runtime_secs=120
# computation_time=(100 200 500 800 1000 1200 1500 2000 5000 4000 3800 3300 3000 2800 2500 2200)
computation_time=(100)
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
    # start_e2e_clients ${client_nodes[2]} $computation_time $runtime_secs 2 10 $benchmark_dir/logs/
    # start_e2e_clients ${client_nodes[3]} $computation_time $runtime_secs 3 10 $benchmark_dir/logs/
    echo "Waiting for clients to terminate"

    wait 

    cleanup_clients
    cleanup_servers
    collect_logs $num_shards

    # move logs to a different folder
    mkdir -p "$benchmark_dir/results/e2e_${computation_time}"
    mv $benchmark_dir/logs/* "$benchmark_dir/results/e2e_${computation_time}"
done

