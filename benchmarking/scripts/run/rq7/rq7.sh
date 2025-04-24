#!/bin/bash

five_shard=true
source ../../common.sh 
pushd $benchmark_dir/scripts

# parameters
runtime_secs=120
computation_time=(1200)
num_shards=(1 2 3 4 5)

for ct in "${computation_time[@]}";
do 
    for shards in "${num_shards[@]}";
    do 
        if [ "$shards" -ge 3 ]; then 
                # switch to the staggered version 
                sed -i 's/const staggeringFactor int64 = -1/const staggeringFactor int64 = 2/' ../../order/order_server.go
                sed -i 's/const staggeringFactor int64 = -1/const staggeringFactor int64 = 2/' ../../data/data_server.go

                pushd $benchmark_dir/../ 
                go build
                popd 

                # wait for NFS to sync
                sleep 5 
        fi
        for iter in $(seq 1 3);
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

            suffix="wo_sc"
            if [ "$shards" -ge 3 ]; then 
                suffix="wi_sc"
            fi
            mkdir -p "$benchmark_dir/results/e2e_scalability/runs_3_${suffix}/$iter/e2e_${ct}_${shards}"
            mv $benchmark_dir/logs/* "$benchmark_dir/results/e2e_scalability/runs_3_${suffix}/$iter/e2e_${ct}_${shards}"
        done
        if [ "$shards" -ge 3 ]; then 
            # switch back 
            sed -i 's/const staggeringFactor int64 = 2/const staggeringFactor int64 = -1/' ../../order/order_server.go
            sed -i 's/const staggeringFactor int64 = 2/const staggeringFactor int64 = -1/' ../../data/data_server.go
            pushd $benchmark_dir/../ 
            go build
            popd 

            # wait for NFS to sync
            sleep 5 
        fi
    done 
done

popd