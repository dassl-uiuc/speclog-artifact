#!/bin/bash

five_shard=true
source ../../common.sh 
pushd $benchmark_dir/scripts

# parameters
runtime_secs=120
computation_time=(1200)
num_shards=(1 2 3 4 5)
num_iter=3

for ct in "${computation_time[@]}";
do 
    for shards in "${num_shards[@]}";
    do 
        for iter in $(seq 1 $num_iter);
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

            suffix="scalog"
            mkdir -p "$results_dir/e2e_scalability/runs_3_${suffix}/$iter/e2e_${ct}_${shards}"
            mv $benchmark_dir/logs/* "$results_dir/e2e_scalability/runs_3_${suffix}/$iter/e2e_${ct}_${shards}"
        done
    done 
done

popd