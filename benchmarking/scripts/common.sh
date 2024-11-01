#!/bin/bash
PASSLESS_ENTRY="/users/sgbhat3/.ssh/id_rsa"

benchmark_dir="/proj/rasl-PG0/sgbhat3/speclog/benchmarking"
LOGDIR="/data"

# index into remote_nodes/ips for order nodes
order=("node0" "node1" "node2")

# index into remote_nodes/ips for data shards
data_0=("node3" "node4")
# data_1=("node5" "node6")

client_nodes=("node7" "node8")

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
    for svr in ${data_0[@]};
    do 
        scp -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY sgbhat3@$svr:/data/*.log $benchmark_dir/logs/ &
    done 
    # for svr in ${data_1[@]};
    # do 
    #     scp -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY sgbhat3@$svr:/data/*.log $benchmark_dir/logs/ &
    # done 
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

start_data_nodes() {
    # start data nodes
    for ((i=0; i<=1; i++))
    do
        echo "Starting data-0-${i} on ${data_0[$i]}"
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_0[$i]} "sh -c \"cd $benchmark_dir/data-0-$i; nohup ./run_goreman.sh > ${LOGDIR}/data-0-$i.log 2>&1 &\""
    done

    # for ((i=0; i<=1; i++))
    # do
    #     echo "Starting data-1-${i} on ${data_1[$i]}"
    #     ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_1[$i]} "sh -c \"cd $benchmark_dir/data-1-$i; nohup ./run_goreman.sh > ${LOGDIR}/data-1-$i.log 2>&1 &\""
    # done
}

start_discovery() {
    # start discovery
    echo "Starting discovery on ${data_0[0]}"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_0[0]} "sh -c \"cd $benchmark_dir/disc; nohup ./run_goreman.sh > ${LOGDIR}/disc.log 2>&1 &\""
}

check_data_log() {
    for ((i=0; i<=1; i++))
    do
        echo "Checking data node data-0-$i..."
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_0[$i]} "grep error ${LOGDIR}/data-0-$i.log"
    done

    # for ((i=0; i<=1; i++))
    # do
    #     echo "Checking data node data-1-$i..."
    #     ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_1[$i]} "grep error ${LOGDIR}/data-1-$i.log"
    # done
}


start_append_clients() {
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; ./run_append_client.sh $2 $3 $1 $4 $5 $6 $7 $8 > ${LOGDIR}/client_$1.log 2>&1" &
}

# args: client node, computation time, runtime secs
start_e2e_clients() {
    go_path="/usr/local/go/bin/go"
    ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY $1 "cd $benchmark_dir/scripts; $go_path run single_client_e2e.go $2 $3 > ${LOGDIR}/client_$1.log 2>&1" &
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

monitor_disk_stats() {
    for ((i=0; i<=1; i++))
    do
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_0[$i]} "sh -c \"nohup dstat --output ${LOGDIR}/data-0-$i.csv 5 > /dev/null 2>&1  &\""
    done
}

get_disk_stats() {
    for ((i=0; i<=1; i++))
    do 
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_0[$i]} "sudo pkill -f \"dstat\""
        ssh -o StrictHostKeyChecking=no -i $PASSLESS_ENTRY ${data_0[$i]} "mv ${LOGDIR}/data-0-$i.csv $benchmark_dir/$1"
    done
}