#!/bin/bash

five_shard=true
source ../../common.sh 
pushd $benchmark_dir/scripts

num_shards=$1

if [ "$num_shards" -gt 3 ]; then 
    # switch to the staggered version 
    sed -i 's/const staggeringFactor int64 = -1/const staggeringFactor int64 = 2/' ../../order/order_server.go
    sed -i 's/const staggeringFactor int64 = -1/const staggeringFactor int64 = 2/' ../../data/data_server.go

    pushd $benchmark_dir/../ 
    go build
    popd 

    # wait for NFS to sync
    sleep 5 
fi 

num_iter=3
for iter in $(seq 1 $num_iter); 
do 
    for interval in "${batching_intervals[@]}";
    do
        # modify intervals
        modify_batching_intervals $interval
        c=$((num_shards * 20))
        shard=$num_shards
        rate=1000
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

        num_client_nodes=3
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
            start_append_clients "${client_nodes[$i]}" $num_jobs_for_client "2m" $c $interval $jobs "append" $rate

            jobs=$(($jobs + $num_jobs_for_client))
        done

        echo "Waiting for clients to terminate"
        wait

        cleanup_clients
        cleanup_servers

        # check for errors in log files
        check_data_log $shard
        collect_logs

        append_suffix="wo"
        if [ "$num_shards" -gt 3 ]; then 
            append_suffix="wi"
        fi
        # move logs to a different folder
        mkdir -p "$results_dir/append_${append_suffix}/$iter/$interval/append_bench_${c}"
        mv $benchmark_dir/results/$interval/append_bench_${c}/* $results_dir/append_${append_suffix}/$iter/$interval/append_bench_${c}
        rm -rf $benchmark_dir/results/$interval
    done
done 


if [ "$num_shards" -gt 3 ]; then 
    # switch to the staggered version 
    sed -i 's/const staggeringFactor int64 = 2/const staggeringFactor int64 = -1/' ../../order/order_server.go
    sed -i 's/const staggeringFactor int64 = 2/const staggeringFactor int64 = -1/' ../../data/data_server.go
    pushd $benchmark_dir/../ 
    go build
    popd 

    # wait for NFS to sync
    sleep 5 
fi 

popd