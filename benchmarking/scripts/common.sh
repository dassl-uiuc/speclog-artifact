#!/bin/bash
PASSLESS_ENTRY="/users/sgbhat3/.ssh/id_rsa"

benchmark_dir="/proj/rasl-PG0/sgbhat3/speclog/benchmarking"
LOGDIR="/data"

# index into remote_nodes/ips for order nodes
order=("node0" "node1" "node2")

# index into remote_nodes/ips for data shards
data_pri=("node3" "node5" "node7" "node9")
data_sec=("node4" "node6" "node8" "node10")

client_nodes=("node13" "node14" "node15" "node12")
intrusion_detection_dir="../../applications/vanilla_applications/intrusion_detection"
transaction_analysis_dir="../../applications/vanilla_applications/transaction_analysis"

batching_intervals=("1ms")

modify_batching_intervals() {
    sed -i "s|order-batching-interval: .*|order-batching-interval: $1|" "${benchmark_dir}/../.scalog.yaml"
    sed -i "s|data-batching-interval: .*|data-batching-interval: $1|" "${benchmark_dir}/../.scalog.yaml"
}

clear_server_logs() {
    # mount storage and clear existing logs if any
    ./run_script_on_servers.sh ./setup_disk.sh
}

clear_client_logs() {
    # mount storage and clear existing logs if any
    ./run_script_on_clients.sh ./setup_disk.sh
    ./run_script_on_clients.sh ./client_tmp_clear.sh
}

cleanup_servers() {
    # kill existing servers
    ./run_script_on_servers.sh ./kill_all_goreman.sh
}

cleanup_clients() {
    # kill existing clients
    ./run_script_on_clients.sh ./kill_all_benchmark.sh
}

drop_server_caches() {
    ./run_script_on_servers.sh ./drop_caches.sh
}

collect_logs() {
    for svr in ${order[@]};
    do 
        scp -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY sgbhat3@$svr:/data/*.log $benchmark_dir/logs/ &
    done 
    for svr in ${data_pri[@]};
    do 
        scp -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY sgbhat3@$svr:/data/*.log $benchmark_dir/logs/ &
    done 
    for svr in ${data_sec[@]};
    do 
        scp -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY sgbhat3@$svr:/data/*.log $benchmark_dir/logs/ &
    done 
    for svr in ${client_nodes[@]};
    do 
        scp -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY sgbhat3@$svr:/data/*.log $benchmark_dir/logs/ &
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

# args: client node, runtime secs, shardId, numAppenders, filepath
start_lagfix_clients() {
    go_path="/usr/local/go/bin/go"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; $go_path run lagfix.go $2 $3 $4 $5 > ${LOGDIR}/client_$1_$3.log 2>&1" &
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