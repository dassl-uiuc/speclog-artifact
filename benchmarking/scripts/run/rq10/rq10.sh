#!/bin/bash

# This script tests an application under failure conditions

apps=true
source ../../common.sh
client_nodes=("node13" "node14" "node15" "node12")

pushd $benchmark_dir/scripts

run_ta() {
    num_shards=2
    append_clients=("40")
    read_clients=("4")
    replicas=("4")
    append_type="1"

    sed -i 's/consume-run-time: 120/consume-run-time: 60/' ../../applications/vanilla_applications/transaction_analysis/transaction_analysis_config.yaml
    sed -i 's/num-read-clients: 2/num-read-clients: 4/' ../../applications/vanilla_applications/transaction_analysis/transaction_analysis_config.yaml
    sed -i 's/num-append-clients: 20/num-append-clients: 40/' ../../applications/vanilla_applications/transaction_analysis/transaction_analysis_config.yaml
    sed -i 's/produce-run-time: 120/produce-run-time: 65/' ../../applications/vanilla_applications/transaction_analysis/transaction_analysis_config.yaml

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
            
            mkdir "../../applications/vanilla_applications/transaction_analysis/analytics"
            rm -rf "../../applications/vanilla_applications/transaction_analysis/data"
            mkdir "../../applications/vanilla_applications/transaction_analysis/data"

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

            echo "wait 30s before killing 1 shard"
            # sleep 30s

            # ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_pri[0]} "sudo bash -s" < ./kill_all_goreman.sh
            # ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_sec[0]} "sudo bash -s" < ./kill_all_goreman.sh

            # echo "Shard 0 killed, waiting for clients to finish"

            wait 

            cleanup_clients
            cleanup_servers

            # check for errors in log files
            check_data_log $num_shards
            collect_logs
        done
    done

    # restore yaml file
    sed -i 's/consume-run-time: 60/consume-run-time: 120/' ../../applications/vanilla_applications/transaction_analysis/transaction_analysis_config.yaml
    sed -i 's/num-read-clients: 4/num-read-clients: 2/' ../../applications/vanilla_applications/transaction_analysis/transaction_analysis_config.yaml
    sed -i 's/num-append-clients: 40/num-append-clients: 20/' ../../applications/vanilla_applications/transaction_analysis/transaction_analysis_config.yaml
    sed -i 's/produce-run-time: 65/produce-run-time: 120/' ../../applications/vanilla_applications/transaction_analysis/transaction_analysis_config.yaml
}

run_ta
mkdir -p $results_dir/apps/transaction_analysis_failure/speclog
cp -r ../../applications/vanilla_applications/transaction_analysis/data $results_dir/apps/transaction_analysis_failure/speclog
