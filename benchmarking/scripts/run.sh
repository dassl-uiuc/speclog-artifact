#!/bin/bash

source ./common.sh

# mode 
#   0 -> append one experiment mode
#   1 -> append experiment mode
#   2 -> read experiment mode
#   3 -> setup servers
#   4 -> kill server and client, read server logs for errors
#   5 -> clear server and client logs
#   6 -> intrusion detection experiment mode
#   7 -> transaction analysis experiment mode
#   8 -> hft experiment mode

num_shards=1
mode="$1"
if [ "$mode" -eq 0 ]; then # append one experiment mode
    clients=("60" "320")
    num_shards=("1" "4")
    for interval in "${batching_intervals[@]}";
    do
        # modify intervals
        modify_batching_intervals $interval

        for ((j=0; j<${#num_shards[@]}; j++)) 
        do
            c=${clients[$j]}
            shard=${num_shards[$j]}
            echo "Running append experiment with $shard shards"
            cleanup_clients
            cleanup_servers
            clear_server_logs
            clear_client_logs

            start_order_nodes
            start_discovery
            start_data_nodes $shard
            monitor_disk_stats $shard

            # wait for 10 secs
            sleep 10

            num_client_nodes=${#client_nodes[@]}
            high_num=$((($c + $num_client_nodes - 1)/$num_client_nodes))
            low_num=$(($c / $num_client_nodes))
            mod=$(($c % $num_client_nodes))

            jobs=0

            for (( i=0; i<num_client_nodes; i++))
            do
                if [ "$i" -lt "$mod" ]; then
                    # If there's a remainder, assign one additional job to the first 'mod' clients
                    num_jobs_for_client=$((low_num + 1))
                else
                    num_jobs_for_client=$low_num
                fi
                
                # start_append_clients <client_id> <num_of_clients_to_run> <num_appends_per_client> <total_clients> <interval> <start_sharding_hint> <append_mode> <rate>
                start_append_clients "${client_nodes[$i]}" $num_jobs_for_client "2m" $c $interval $jobs "appendOne" "0"

                jobs=$(($jobs + $num_jobs_for_client))
            done

            echo "Waiting for clients to terminate"
            wait

            cleanup_clients
            cleanup_servers

            # check for errors in log files
            check_data_log $shard
            collect_logs

            # move logs to a different folder
            mkdir -p "$benchmark_dir/results/logs/$interval/append_bench_${c}"
            mv $benchmark_dir/logs/* "$benchmark_dir/results/logs/$interval/append_bench_${c}"
            
            # move iostat dump to results folder
            get_disk_stats "results/$interval/append_bench_$c/" $shard
        done
    done
elif [ "$mode" -eq 1 ]; then # append experiment mode
    # clients=("20" "80")
    # num_shards=("1" "4")
    # rates=("1000" "1050")
    clients=("80")
    num_shards=("4")
    rates=("1050")
    for interval in "${batching_intervals[@]}";
    do
        # modify intervals
        modify_batching_intervals $interval

        for ((j=0; j<${#num_shards[@]}; j++)) 
        do
            c=${clients[$j]}
            shard=${num_shards[$j]}
            echo "Running append experiment with $shard shards"
            cleanup_clients
            cleanup_servers
            clear_server_logs
            clear_client_logs

            start_order_nodes
            start_discovery
            start_data_nodes $shard
            monitor_disk_stats $shard

            # wait for 10 secs
            sleep 10

            num_client_nodes=${#client_nodes[@]}
            high_num=$((($c + $num_client_nodes - 1)/$num_client_nodes))
            low_num=$(($c / $num_client_nodes))
            mod=$(($c % $num_client_nodes))

            jobs=0

            for (( i=0; i<num_client_nodes; i++))
            do
                if [ "$i" -lt "$mod" ]; then
                    # If there's a remainder, assign one additional job to the first 'mod' clients
                    num_jobs_for_client=$((low_num + 1))
                else
                    num_jobs_for_client=$low_num
                fi
                
                # start_append_clients <client_id> <num_of_clients_to_run> <num_appends_per_client> <total_clients> <interval> <start_sharding_hint> <append_mode> <rate>
                start_append_clients "${client_nodes[$i]}" $num_jobs_for_client "2m" $c $interval $jobs "append" ${rates[$j]}

                jobs=$(($jobs + $num_jobs_for_client))
            done

            echo "Waiting for clients to terminate"
            wait

            cleanup_clients
            cleanup_servers

            # check for errors in log files
            check_data_log $shard
            collect_logs

            # move logs to a different folder
            mkdir -p "$benchmark_dir/results/logs/$interval/append_bench_${c}_${rates[$j]}"
            mv $benchmark_dir/logs/* "$benchmark_dir/results/logs/$interval/append_bench_${c}_${rates[$j]}"
            
            # move iostat dump to results folder
            get_disk_stats "results/$interval/append_bench_${c}_${rates[$j]}/" $shard
        done
    done
elif [ "$mode" -eq 2 ]; then # read experiment mode
    clients=("1")
    for interval in "${batching_intervals[@]}";
    do
        # modify intervals
        modify_batching_intervals $interval

        cleanup_clients
        cleanup_servers
        clear_server_logs
        clear_client_logs

        start_order_nodes
        start_discovery
        start_data_nodes $num_shards

        # wait for 10 secs
        sleep 10

        load_phase "4096" "2000000" "10" "gsnToShardMap.txt"

        echo "Done with loading"

        for c in "${clients[@]}"; 
        do 
            drop_server_caches 

            num_client_nodes=${#client_nodes[@]}
            high_num=$((($c + $num_client_nodes - 1)/$num_client_nodes))
            low_num=$(($c / $num_client_nodes))
            mod=$(($c % $num_client_nodes))

            for (( i=0; i<num_client_nodes; i++))
            do
                if [ "$i" -lt "$mod" ]; then
                    # If there's a remainder, assign one additional job to the first 'mod' clients
                    num_jobs_for_client=$((low_num + 1))
                else
                    num_jobs_for_client=$low_num
                fi

                # start_append_clients <client_id> <num_of_clients_to_run> <num_appends_per_client> <total_clients> <input_filename>
                start_sequential_read_clients "${client_nodes[$i]}" $num_jobs_for_client "3m" $c $interval "gsnToShardMap.txt"
            done

            echo "Waiting for clients to terminate"
            wait

            cleanup_clients
        done
        
        cleanup_servers
        # check for errors in log files
        check_data_log $num_shards
    done
elif [ "$mode" -eq 3 ]; then # setup servers mode
    cleanup_clients
    cleanup_servers
    clear_server_logs
    clear_client_logs

    start_order_nodes
    start_discovery
    start_data_nodes $num_shards
elif [ "$mode" -eq 4 ]; then # kill servers and clients 
    cleanup_clients
    cleanup_servers

    # check for errors in log files
    check_data_log $num_shards
elif [ "$mode" -eq 5 ]; then 
    cleanup_clients
    cleanup_servers
    collect_logs
elif [ "$mode" -eq 6 ]; then
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
            
            sudo mkdir "../../applications/vanilla_applications/intrusion_detection/analytics"
            sudo rm -rf "../../applications/vanilla_applications/intrusion_detection/data"
            sudo mkdir "../../applications/vanilla_applications/intrusion_detection/data"

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
                    start_intrusion_detection_clients "${client_nodes[$j]}" $num_replicas $num_read_clients_per_replica $k
                    # Spawn append clients
                    start_intrusion_detection_generator_clients "${client_nodes[$j]}" $num_replicas $num_append_clients_per_replica $append_type $k
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
elif [ "$mode" -eq 7 ]; then
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
elif [ "$mode" -eq 8 ]; then
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
            
            sudo mkdir "../../applications/vanilla_applications/hft/analytics"
            sudo rm -rf "../../applications/vanilla_applications/hft/data"
            sudo mkdir "../../applications/vanilla_applications/hft/data"

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
                    start_hft_clients "${client_nodes[$j]}" $num_replicas $num_read_clients_per_replica $((k * 2))
                    # Spawn append clients
                    start_hft_generator_clients "${client_nodes[$j]}" $num_replicas $num_append_clients_per_replica $append_type $k
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
else # cleanup logs
    clear_server_logs
    clear_client_logs
fi
