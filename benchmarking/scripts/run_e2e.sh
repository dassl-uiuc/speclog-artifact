#!/bin/bash

source ./common.sh

# parameters
runtime_secs=60
computation_time=(1000)
num_shards=(1)

for ct in "${computation_time[@]}";
do 
    for shards in "${num_shards[@]}";
    do 
        cleanup_clients
        cleanup_servers
        clear_server_logs
        clear_client_logs

        start_order_nodes
        start_discovery
        start_data_nodes $shards

        sleep 5
        num_clients=$((2*$shards))
        for (( i = 0; i < $num_clients; i++ )); do
            start_e2e_clients ${client_nodes[i % ${#client_nodes[@]}]} $ct $runtime_secs $i 10 $benchmark_dir/logs/
        done
        echo "Waiting for clients to terminate"

        wait 

        cleanup_clients
        cleanup_servers
        collect_logs $shards

        # move logs to a different folder
        mkdir -p "$benchmark_dir/results/e2e_${ct}"
        mv $benchmark_dir/logs/* "$benchmark_dir/results/e2e_${ct}"
    done 
done

