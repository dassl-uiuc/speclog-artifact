#!/bin/bash

# WARNING: Please setup .env before running this script

# index into remote_nodes/ips for order nodes
order=("node0" "node1" "node2")

# index into remote_nodes/ips for data shards
if [ "$five_shard" = "true" ]; then 
    data_pri=("node3" "node5" "node7" "node9" "node11")
    data_sec=("node4" "node6" "node8" "node10" "node12")

    client_nodes=("node13" "node14" "node15")
    
    run_server_suffix="13"
    run_client_suffix="3"
elif [ "$apps" = "true" ]; then 
    data_pri=("node3" "node5")
    data_sec=("node4" "node6")

    client_nodes=("node13" "node14")
    
    run_server_suffix="7"
    run_client_suffix="2"
else 
    data_pri=("node3" "node5" "node7" "node9")
    data_sec=("node4" "node6" "node8" "node10")

    client_nodes=("node13" "node14" "node15" "node12")
    run_server_suffix="11"
    run_client_suffix="4"
fi 
intrusion_detection_dir="../../applications/vanilla_applications/intrusion_detection"
transaction_analysis_dir="../../applications/vanilla_applications/transaction_analysis"
hft_dir="../../applications/vanilla_applications/hft"

batching_intervals=("1ms")

check_sync() {
    shas=$1
    first_sha=$(echo "$shas" | head -n 1)
    while read -r sha; do
        if [ "$sha" != "$first_sha" ]; then
            echo "Error: NFS out of sync"
            exit 1
        fi
    done <<< "$shas"
}

modify_batching_intervals() {
    sed -i "s|order-batching-interval: .*|order-batching-interval: $1|" "${benchmark_dir}/../.scalog.yaml"
    sed -i "s|data-batching-interval: .*|data-batching-interval: $1|" "${benchmark_dir}/../.scalog.yaml"
}

clear_server_logs() {
    # mount storage and clear existing logs if any
    ./run_script_on_servers.sh ./setup_disk.sh ${run_server_suffix}
}

clear_client_logs() {
    # mount storage and clear existing logs if any
    ./run_script_on_clients.sh ./setup_disk.sh ${run_client_suffix}
    ./run_script_on_clients.sh ./client_tmp_clear.sh ${run_client_suffix}
}

cleanup_servers() {
    # kill existing servers
    ./run_script_on_servers.sh ./kill_all_goreman.sh ${run_server_suffix}
}

cleanup_clients() {
    # kill existing clients
    ./run_script_on_clients.sh ./kill_all_benchmark.sh ${run_client_suffix}
}

drop_server_caches() {
    ./run_script_on_servers.sh ./drop_caches.sh ${run_server_suffix}
}

set_bool_variable_in_file() {
    local file="$1"
    local var="$2"
    local value="$3"

    if [[ ! -f "$file" ]]; then
        echo "File '$file' does not exist."
        return 1
    fi

    echo "Setting $var to $value in $file"

    sed -i -E "s/^const[[:space:]]+$var[[:space:]]+bool[[:space:]]*=[[:space:]]*(true|false)/const $var bool = $value/" "$file"
}

collect_logs() {
    for svr in ${order[@]};
    do 
        scp -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $svr:/data/*.log $benchmark_dir/logs/ &
    done 
    for svr in ${data_pri[@]};
    do 
        scp -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $svr:/data/*.log $benchmark_dir/logs/ &
    done 
    for svr in ${data_sec[@]};
    do 
        scp -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $svr:/data/*.log $benchmark_dir/logs/ &
    done 
    for svr in ${client_nodes[@]};
    do 
        scp -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $svr:/data/*.log $benchmark_dir/logs/ &
    done
    wait
}

start_order_nodes() {
    # start order nodes
    for ((i=0; i<=2; i++))
    do
        echo "Starting order-${i} on ${order[$i]}"
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${order[$i]} "sh -c \"cd $benchmark_dir/order-$i; nohup ./run_goreman.sh > ${LOGDIR}/order-$i.log 2>&1 &\""
    done
}

# args: numshards
start_data_nodes() {
    # start data nodes
    for ((i=0; i<$1; i++))
    do
        echo "Starting primary for shard $i on ${data_pri[$i]}"
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_pri[$i]} "sh -c \"cd $benchmark_dir/data-$i-0; nohup ./run_goreman.sh > ${LOGDIR}/data-$i-0.log 2>&1 &\""
    done

    for ((i=0; i<$1; i++))
    do
        echo "Starting secondary for shard $i on ${data_sec[$i]}"
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_sec[$i]} "sh -c \"cd $benchmark_dir/data-$i-1; nohup ./run_goreman.sh > ${LOGDIR}/data-$i-1.log 2>&1 &\""
    done
}

## Note: Always start discovery before starting data nodes
start_discovery() {
    # start discovery
    echo "Starting discovery on ${data_pri[0]}"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_pri[0]} "sh -c \"cd $benchmark_dir/disc; nohup ./run_goreman.sh > ${LOGDIR}/disc.log 2>&1 &\""
    sleep 1
}

# args: numshards
check_data_log() {
    for ((i=0; i<$1; i++))
    do
        echo "Checking data node data-$i-0..."
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_pri[$i]} "grep error ${LOGDIR}/data-$i-0.log"
    done

    for ((i=0; i<$1; i++))
    do
        echo "Checking data node data-$i-1..."
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_sec[$i]} "grep error ${LOGDIR}/data-$i-1.log"
    done
}


start_append_clients() {
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; ./run_append_client.sh $2 $3 $1 $4 $5 $6 $7 $8 > ${LOGDIR}/client_$1.log 2>&1" &
}

# args: client node, computation time, runtime secs, shardId, numAppenders, filepath
start_e2e_clients() {
    go_path="/usr/local/go/bin/go"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; $go_path run single_client_e2e.go $2 $3 $4 $5 $6 > ${LOGDIR}/client_$1_$4.log 2>&1" &
}

# args: client node, computation time, runtime secs, shardId, numAppenders, filepath
start_straggler_clients() {
    go_path="/usr/local/go/bin/go"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; $go_path run straggler.go $2 $3 $4 $5 $6 > ${LOGDIR}/client_$1_$4.log 2>&1" &
}

# args: client node, computation time, runtime secs, new client runtime (s1), numAppendersPerShard, filepath, withConsumer
start_reconfig_clients() {
    go_path="/usr/local/go/bin/go"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; $go_path run reconfig.go $2 $3 $4 $5 $6 $7 > ${LOGDIR}/client_$1.log 2>&1" &
}

# args: client node, runtime secs, shardId, numAppenders, clientId, filepath
start_qc_clients() {
    go_path="/usr/local/go/bin/go"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; $go_path run quota_change.go $2 $3 $4 $5 $6 > ${LOGDIR}/client_$1_$3_$5.log 2>&1" &
}

# args: client node, runtime secs, numAppendersPerShard, new client join time, computation time, filepath
start_qc_e2e_clients() {
    go_path="/usr/local/go/bin/go"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; $go_path run quota_change_e2e.go $2 $3 $4 $5 $6 > ${LOGDIR}/client_$1.log 2>&1" &
}

# args: client node, runtime secs, shardId, numAppenders, filepath
start_lagfix_clients() {
    go_path="/usr/local/go/bin/go"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; $go_path run lagfix.go $2 $3 $4 $5 > ${LOGDIR}/client_$1_$3.log 2>&1" &
}

# args: client node, runtime secs, numAppendersPerShard, computationTime, filepath
start_lagfix_e2e_clients() {
    go_path="/usr/local/go/bin/go"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; $go_path run lagfix_e2e.go $2 $3 $4 $5 > ${LOGDIR}/client_$1.log 2>&1" &
}

start_random_read_clients() {
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; ./run_random_read_client.sh $2 $3 $1 $4 $5 $6 > ${LOGDIR}/client_$1.log 2>&1" &
}

start_sequential_read_clients() {
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; ./run_sequential_read_client.sh $2 $3 $1 $4 $5 $6 > ${LOGDIR}/client_$1.log 2>&1" &
}

load_phase() {
    /usr/local/go/bin/go run load.go $1 $2 $3 $4 
}

# args: numshards
monitor_disk_stats() {
    for ((i=0; i<$1; i++))
    do
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_pri[$i]} "sh -c \"nohup dstat --output ${LOGDIR}/data-$i-0.csv 5 > /dev/null 2>&1  &\""
    done
}

# args: path, numshards
get_disk_stats() {
    for ((i=0; i<$2; i++))
    do 
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_pri[$i]} "sudo pkill -f \"dstat\""
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_pri[$i]} "mv ${LOGDIR}/data-$i-0.csv $benchmark_dir/$1"
    done
}

# args: shard idx
start_specific_shard() {
    # start data nodes
    i=$1
    echo "Starting primary for shard $i on ${data_pri[$i]}"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_pri[$i]} "sh -c \"cd $benchmark_dir/data-$i-0; nohup ./run_goreman.sh > ${LOGDIR}/data-$i-0.log 2>&1 &\""

    echo "Starting secondary for shard $i on ${data_sec[$i]}"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_sec[$i]} "sh -c \"cd $benchmark_dir/data-$i-1; nohup ./run_goreman.sh > ${LOGDIR}/data-$i-1.log 2>&1 &\""
}

start_intrusion_detection_clients() {
    # echo "Executing: ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 'cd $benchmark_dir/scripts; sudo $intrusion_detection_dir/intrusion_detection_client.sh $1 $2 $3 $4> ${LOGDIR}/client_$1.log 2>&1'"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; sudo $intrusion_detection_dir/intrusion_detection_client.sh $1 $2 $3 $4 > ${LOGDIR}/client_reader_$1 2>&1" &
}

start_intrusion_detection_generator_clients() {
    # echo "Executing: ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 'cd $benchmark_dir/scripts; sudo $intrusion_detection_dir/intrusion_detection_generator_client.sh $1 $2 $3 $4 $5> ${LOGDIR}/client_$1.log 2>&1'"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; sudo $intrusion_detection_dir/intrusion_detection_generator_client.sh $1 $2 $3 $4 $5 > ${LOGDIR}/client_writer_$1 2>&1" &
}

start_transaction_analysis_clients() {
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; sudo $transaction_analysis_dir/transaction_analysis_client.sh $1 $2 $3 $4 > ${LOGDIR}/client_reader_$1_$4.log 2>&1" &
}

start_transaction_analysis_generator_clients() {
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; sudo $transaction_analysis_dir/transaction_analysis_generator_client.sh $1 $2 $3 $4 $5 > ${LOGDIR}/client_writer_$1_$5.log 2>&1" &
}

start_hft_clients() {
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; sudo $hft_dir/hft_client.sh $1 $2 $3 $4 > ${LOGDIR}/client_reader_$1_$4.log 2>&1" &
}

start_hft_generator_clients() {
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; sudo $hft_dir/hft_generator_client.sh $1 $2 $3 $4 $5 > ${LOGDIR}/client_writer_$1_$5.log 2>&1" &
}
