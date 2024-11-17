#!/bin/bash

source ./common.sh
# mode 
#   0 -> append one experiment mode
#   1 -> append experiment mode
#   2 -> read experiment mode
#   3 -> setup servers
#   4 -> kill server and client, read server logs for errors
#   5 -> clear server and client logs

num_shards=1
mode="$1"
if [ "$mode" -eq 0 ]; then # append one experiment mode
    # clients=("130")
    clients=("80")
    for interval in "${batching_intervals[@]}";
    do
        # modify intervals
        modify_batching_intervals $interval

        for c in "${clients[@]}"; 
        do
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
            check_data_log $num_shards
            collect_logs
            
            # move iostat dump to results folder
            get_disk_stats "results/$interval/append_bench_$c/" $num_shards
        done
    done
elif [ "$mode" -eq 1 ]; then # append experiment mode
    clients=("80" "380")
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
                start_append_clients "${client_nodes[$i]}" $num_jobs_for_client "2m" $c $interval $jobs "append" "260"

                jobs=$(($jobs + $num_jobs_for_client))
            done

            echo "Waiting for clients to terminate"
            wait

            cleanup_clients
            cleanup_servers

            # check for errors in log files
            check_data_log $shard
            collect_logs
            
            # move iostat dump to results folder
            get_disk_stats "results/$interval/append_bench_$c/" $shard
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
else # cleanup logs
    clear_server_logs
    clear_client_logs
fi
