#!/bin/bash

apps=true
source ../../common.sh
pushd $benchmark_dir/scripts

run_ta() {
    num_shards=1
    append_clients=("20")
    read_clients=("2")
    replicas=("2")
    append_type="1"
    for interval in "${batching_intervals[@]}";
    do
        # modify intervals
        modify_batching_intervals $interval
 
        for i in "${!append_clients[@]}"; 
        do
            num_append_clients=${append_clients[$i]}
            num_read_clients=${read_clients[$i]}
            num_replicas=${replicas[$i]}
            num_append_clients_per_replica=$(($num_append_clients / $num_replicas))
            num_read_clients_per_replica=$(($num_read_clients / $num_replicas))

            cleanup_clients
            cleanup_servers
            clear_server_logs
            clear_client_logs

            start_order_nodes
            start_discovery
            start_data_nodes $num_shards
            monitor_disk_stats $num_shards

            # wait for 10 secs
            sleep 10
            
            sudo mkdir "../../applications/vanilla_applications/transaction_analysis/analytics"
            sudo rm -rf "../../applications/vanilla_applications/transaction_analysis/data"
            sudo mkdir "../../applications/vanilla_applications/transaction_analysis/data"

            # Ensure even division between num_replica and client_nodes
            if (( $num_replicas % ${#client_nodes[@]} != 0 )); then
                echo "Error: num_replica ($num_replica) is not evenly divisible by the number of client nodes (${#client_nodes[@]})."
                exit 1
            fi

            if (( $num_append_clients % $num_replicas != 0 )); then
                echo "Error: num_append_clients ($num_append_clients) is not evenly divisible by num_replicas ($num_replicas)."
                exit 1
            fi

            if (( $num_read_clients % $num_replicas != 0 )); then
                echo "Error: num_read_clients ($num_read_clients) is not evenly divisible by num_replicas ($num_replicas)."
                exit 1
            fi

            start=0
            # TODO: Kind of assuming that we have even number num replica and length of client nodes
            stride=$(( $num_replicas / ${#client_nodes[@]} ))
            for j in "${!client_nodes[@]}";
            do
                for (( k=start; k<start+stride; k++ ));
                do
                    # Spawn reader clients
                    start_transaction_analysis_clients "${client_nodes[$j]}" $num_replicas $num_read_clients_per_replica $k
                    # Spawn append clients
                    start_transaction_analysis_generator_clients "${client_nodes[$j]}" $num_replicas $num_append_clients_per_replica $append_type $k
                done
                start=$((start + stride))
            done

            echo "Waiting for clients to terminate"
            wait

            cleanup_clients
            cleanup_servers

            # check for errors in log files
            check_data_log $num_shards
            collect_logs
        done
    done
}

n=$1
for i in $(seq 1 $n)
do
    echo "Running $i-th time"
    run_ta
    mkdir -p $benchmark_dir/results/apps/transaction_analysis/speclog_$i
    sudo cp -r ../../applications/vanilla_applications/transaction_analysis/data $benchmark_dir/results/apps/transaction_analysis/speclog_$i
done

popd
