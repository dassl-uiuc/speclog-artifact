#!/bin/bash

source ./common.sh

shard=2
c=40
rate=400
interval=1ms

modify_batching_intervals $interval

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
    start_append_clients "${client_nodes[$i]}" $num_jobs_for_client "1m" $c $interval $jobs "append" $rate

    jobs=$(($jobs + $num_jobs_for_client))
done

echo "Wait for 10s before killing 1 shard"
sleep 30

ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_pri[0]} "sudo bash -s" < ./kill_all_goreman.sh
ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_sec[0]} "sudo bash -s" < ./kill_all_goreman.sh

echo "Shard 0 killed, waiting for clients to finish"
wait

cleanup_clients
cleanup_servers

# check for errors in log files
check_data_log $shard
collect_logs

# move logs to a different folder
mkdir -p "$benchmark_dir/results/logs/$interval/append_bench_${c}_${rate}"
mv $benchmark_dir/logs/* "$benchmark_dir/results/logs/$interval/append_bench_${c}_${rate}"

# move iostat dump to results folder
get_disk_stats "results/$interval/append_bench_${c}_${rate}/" $shard

python3 ./draw_failure.py