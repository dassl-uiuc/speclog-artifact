#!/bin/bash
PASSLESS_ENTRY="/users/sgbhat3/.ssh/id_rsa"

benchmark_dir="/proj/rasl-PG0/sgbhat3/scalog/lazylog-benchmarking"

# index into remote_nodes/ips for order nodes
order=("159" "038" "036")

# index into remote_nodes/ips for data shards
data_0=("124" "123")
data_1=("126" "039")

client_nodes=("136" "034" "007" "158")

batching_intervals=("0.1ms" "10ms")

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

start_order_nodes() {
    # start order nodes
    for ((i=0; i<=2; i++))
    do
        echo "Starting order-${i} on sgbhat3@hp${order[$i]}.utah.cloudlab.us"
        ssh -i $PASSLESS_ENTRY "sgbhat3@hp${order[$i]}.utah.cloudlab.us" "sh -c \"cd $benchmark_dir/order-$i; nohup sudo ./run_goreman.sh > /users/sgbhat3/scalog-storage/order-$i.log 2>&1 &\""
    done
}

start_data_nodes() {
    # start data nodes
    for ((i=0; i<=1; i++))
    do
        echo "Starting data-0-${i} on sgbhat3@hp${data_0[$i]}.utah.cloudlab.us"
        ssh -i $PASSLESS_ENTRY "sgbhat3@hp${data_0[$i]}.utah.cloudlab.us" "sh -c \"cd $benchmark_dir/data-0-$i; nohup sudo ./run_goreman.sh > /users/sgbhat3/scalog-storage/data-0-$i.log 2>&1 &\""
    done

    for ((i=0; i<=1; i++))
    do
        echo "Starting data-1-${i} on sgbhat3@hp${data_1[$i]}.utah.cloudlab.us"
        ssh -i $PASSLESS_ENTRY "sgbhat3@hp${data_1[$i]}.utah.cloudlab.us" "sh -c \"cd $benchmark_dir/data-1-$i; nohup sudo ./run_goreman.sh > /users/sgbhat3/scalog-storage/data-1-$i.log 2>&1 &\""
    done
}

start_discovery() {
    # start discovery
    echo "Starting discovery on sgbhat3@hp${data_0[0]}.utah.cloudlab.us"
    ssh -i $PASSLESS_ENTRY "sgbhat3@hp${data_0[0]}.utah.cloudlab.us" "sh -c \"cd $benchmark_dir/disc; nohup sudo ./run_goreman.sh > /users/sgbhat3/scalog-storage/disc.log 2>&1 &\""
}

check_data_log() {
    for ((i=0; i<=1; i++))
    do
        echo "Checking data node $i..."
        ssh -i $PASSLESS_ENTRY "sgbhat3@hp${data_0[$i]}.utah.cloudlab.us" "grep error /users/sgbhat3/scalog-storage/data-0-$i.log"
    done
}

start_client() {
    ssh -i $PASSLESS_ENTRY sgbhat3@hp$1.utah.cloudlab.us "cd $benchmark_dir/scripts; sudo ./run_client.sh $2 $3 $1 $4 $5 > /users/sgbhat3/scalog-storage/client_$1.log 2>&1" &
}

# clients
clients=("2100" "1800" "1600" "1200" "1000" "800" "600" "512" "256" "128" "64" "32" "16" "8" "4")

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

        # wait for 10 secs
        sleep 10

        num_client_nodes=${#client_nodes[@]}
        for client_node in "${client_nodes[@]}";
        do
            # start_client <client_id> <num_of_clients_to_run> <num_appends_per_client> <total_clients>
            start_client $client_node $(($c/$num_client_nodes)) "4m" $c $interval
        done

        echo "Waiting for clients to terminate"
        wait

        cleanup_clients
        cleanup_servers

        # check for errors in log files
        check_data_log
    done
done