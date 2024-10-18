#!/bin/bash
PASSLESS_ENTRY="/users/tshong/.ssh/id_rsa"

benchmark_dir="/proj/rasl-PG0/tshong/speclog/benchmarking"
LOGDIR="/data"

# index into remote_nodes/ips for order nodes
order=("node0" "node1" "node2")

# index into remote_nodes/ips for data shards
data_0=("node3" "node4")
data_1=("node5" "node6")

client_nodes=("node7")

intrusion_detection_client_nodes=("node7")
intrusion_detection_generator_client_nodes=("node8")
intrusion_detection_dir="/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection"

batching_intervals=("1ms")

modify_batching_intervals() {
    sed -i "s|order-batching-interval: .*|order-batching-interval: $1|" "${benchmark_dir}/../.scalog.yaml"
    sed -i "s|data-batching-interval: .*|data-batching-interval: $1|" "${benchmark_dir}/../.scalog.yaml"
}

clear_server_logs() {
    # mount storage and clear existing logs if any
    sudo ./run_script_on_servers.sh ./setup_disk.sh
}

clear_client_logs() {
    # mount storage and clear existing logs if any
    sudo ./run_script_on_clients.sh ./setup_disk.sh
}

cleanup_servers() {
    # kill existing servers
    sudo ./run_script_on_servers.sh ./kill_all_goreman.sh
}

cleanup_clients() {
    # kill existing clients
    sudo ./run_script_on_clients.sh ./kill_all_benchmark.sh
}

drop_server_caches() {
    sudo ./run_script_on_servers.sh ./drop_caches.sh
}

collect_logs() {
    for svr in ${order[@]};
    do 
        scp -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY tshong@$svr:/data/*.log $benchmark_dir/logs/ &
    done 
    for svr in ${data_0[@]};
    do 
        scp -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY tshong@$svr:/data/*.log $benchmark_dir/logs/ &
    done 
    for svr in ${data_1[@]};
    do 
        scp -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY tshong@$svr:/data/*.log $benchmark_dir/logs/ &
    done 
    wait
}

start_order_nodes() {
    # start order nodes
    for ((i=0; i<=2; i++))
    do
        echo "Starting order-${i} on ${order[$i]}"
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${order[$i]} "sh -c \"cd $benchmark_dir/order-$i; nohup sudo ./run_goreman.sh > ${LOGDIR}/order-$i.log 2>&1 &\""
    done
}

start_data_nodes() {
    # start data nodes
    for ((i=0; i<=1; i++))
    do
        echo "Starting data-0-${i} on ${data_0[$i]}"
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_0[$i]} "sh -c \"cd $benchmark_dir/data-0-$i; nohup sudo ./run_goreman.sh > ${LOGDIR}/data-0-$i.log 2>&1 &\""
    done

    for ((i=0; i<=1; i++))
    do
        echo "Starting data-1-${i} on ${data_1[$i]}"
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_1[$i]} "sh -c \"cd $benchmark_dir/data-1-$i; nohup sudo ./run_goreman.sh > ${LOGDIR}/data-1-$i.log 2>&1 &\""
    done
}

start_discovery() {
    # start discovery
    echo "Starting discovery on ${data_0[0]}"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_0[0]} "sh -c \"cd $benchmark_dir/disc; nohup sudo ./run_goreman.sh > ${LOGDIR}/disc.log 2>&1 &\""
}

check_data_log() {
    for ((i=0; i<=1; i++))
    do
        echo "Checking data node data-0-$i..."
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_0[$i]} "grep error ${LOGDIR}/data-0-$i.log"
    done

    for ((i=0; i<=1; i++))
    do
        echo "Checking data node data-1-$i..."
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_1[$i]} "grep error ${LOGDIR}/data-1-$i.log"
    done
}


start_append_clients() {
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; sudo ./run_append_client.sh $2 $3 $1 $4 $5 > ${LOGDIR}/client_$1.log 2>&1" &
}

start_random_read_clients() {
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; sudo ./run_random_read_client.sh $2 $3 $1 $4 $5 $6 > ${LOGDIR}/client_$1.log 2>&1" &
}

start_sequential_read_clients() {
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; sudo ./run_sequential_read_client.sh $2 $3 $1 $4 $5 $6 > ${LOGDIR}/client_$1.log 2>&1" &
}

load_phase() {
    sudo /usr/local/go/bin/go run load.go $1 $2 $3 $4 
}

monitor_disk_stats() {
    for ((i=0; i<=1; i++))
    do
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_0[$i]} "sh -c \"nohup sudo dstat --output ${LOGDIR}/data-0-$i.csv 5 > /dev/null 2>&1  &\""
    done
}

get_disk_stats() {
    for ((i=0; i<=1; i++))
    do 
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_0[$i]} "sudo pkill -f \"dstat\""
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_0[$i]} "sudo mv ${LOGDIR}/data-0-$i.csv $benchmark_dir/$1"
    done
}

start_intrusion_detection_clients() {
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; sudo $intrusion_detection_dir/intrusion_detection_client.sh $1 $2 > ${LOGDIR}/client_$1.log 2>&1" &
}

start_intrusion_detection_generator_clients() {
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; sudo $intrusion_detection_dir/intrusion_detection_generator_client.sh $1 $2 > ${LOGDIR}/client_$1.log 2>&1" &
}

# mode 
#   0 -> append experiment mode
#   1 -> read experiment mode
#   2 -> setup servers
#   3 -> kill server and client, read server logs for errors
#   4 -> clear server and client logs
#   6 -> intrusion detection experiment mode

mode="$1"
if [ "$mode" -eq 0 ]; then # append experiment mode
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
            start_data_nodes 
            start_discovery
            monitor_disk_stats

            # wait for 10 secs
            sleep 10

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
                
                # start_append_clients <client_id> <num_of_clients_to_run> <num_appends_per_client> <total_clients> <interval>
                start_append_clients "${client_nodes[$i]}" $num_jobs_for_client "2m" $c $interval
            done

            echo "Waiting for clients to terminate"
            wait

            cleanup_clients
            cleanup_servers

            # check for errors in log files
            check_data_log
            collect_logs
            
            # move iostat dump to results folder
            get_disk_stats "results/$interval/append_bench_$c/"
        done
    done
elif [ "$mode" -eq 1 ]; then # read experiment mode
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
        start_data_nodes 
        start_discovery

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
        check_data_log
    done
elif [ "$mode" -eq 2 ]; then # setup servers mode
    cleanup_clients
    cleanup_servers
    clear_server_logs
    clear_client_logs

    start_order_nodes
    start_data_nodes 
    start_discovery
elif [ "$mode" -eq 3 ]; then # kill servers and clients 
    cleanup_clients
    cleanup_servers

    # check for errors in log files
    check_data_log
elif [ "$mode" -eq 4 ]; then 
    cleanup_clients
    cleanup_servers
    collect_logs
elif [ "$mode" -eq 6 ]; then
    append_clients=("4")
    read_clients=("4")
    for interval in "${batching_intervals[@]}";
    do
        # modify intervals
        modify_batching_intervals $interval

        for i in "${!append_clients[@]}"; 
        do
            num_append_clients="${append_clients[$i]}"
            num_read_clients="${read_clients[$i]}"

            cleanup_clients
            cleanup_servers
            clear_server_logs
            clear_client_logs

            start_order_nodes
            start_data_nodes 
            start_discovery
            monitor_disk_stats

            # wait for 10 secs
            sleep 10

            sudo mkdir "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/analytics"
            sudo rm "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/analytics/read_throughput.txt"
            sudo rm "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/analytics/e2e_latencies.txt"
            sudo rm "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/analytics/append_throughput.txt"

            # Spawn reader clients
            num_reader_client_nodes=${#intrusion_detection_client_nodes[@]}

            for (( i=0; i<num_reader_client_nodes; i++))
            do
                # start_fraud_detection_clients <num_of_clients_to_run>
                start_intrusion_detection_clients "${intrusion_detection_client_nodes[$i]}" $num_read_clients
            done

            # Spawn append clients
            num_append_client_nodes=${#intrusion_detection_generator_client_nodes[@]}
            high_num=$((($num_append_clients + $num_append_client_nodes - 1)/$num_append_client_nodes))
            low_num=$(($num_append_clients / $num_append_client_nodes))
            mod=$(($num_append_clients % $num_append_client_nodes))

            for (( i=0; i<num_append_client_nodes; i++))
            do
                if [ "$i" -lt "$mod" ]; then
                    # If there's a remainder, assign one additional job to the first 'mod' clients
                    num_jobs_for_client=$((low_num + 1))
                else
                    num_jobs_for_client=$low_num
                fi
                
                # start_append_clients <num_of_clients_to_run>
                start_intrusion_detection_generator_clients "${intrusion_detection_generator_client_nodes[$i]}" $num_jobs_for_client
            done

            echo "Waiting for clients to terminate"
            wait

            cleanup_clients
            cleanup_servers

            # check for errors in log files
            check_data_log
            collect_logs
        done
    done
else # cleanup logs
    clear_server_logs
    clear_client_logs
fi
