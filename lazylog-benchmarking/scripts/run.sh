#!/bin/bash
PASSLESS_ENTRY="/users/sgbhat3/.ssh/id_rsa"

# remote_nodes=("034" "078" "097" "007" "090" "030" "096" "116")
# ips=("3" "4" "2" "1" "5" "6" "7" "8")

benchmark_dir="/proj/rasl-PG0/sgbhat3/scalog/lazylog-benchmarking"

# index into remote_nodes/ips for order nodes
order=("007" "090" "030")

# index into remote_nodes/ips for data shards
data_0=("096" "116")

client_nodes=("034" "078")

cleanup_servers() {
    # kill existing servers
    sudo ./run_script_on_all.sh ./kill_all_goreman.sh

    # mount storage and clear existing logs if any
    sudo ./run_script_on_all.sh ./setup_disk.sh
}

cleanup_servers_wo_log_clear() {
    # kill existing servers
    sudo ./run_script_on_all.sh ./kill_all_goreman.sh
}

cleanup_client() {
    ssh -i $PASSLESS_ENTRY "sgbhat3@hp$1.utah.cloudlab.us" "cd $benchmark_dir/scripts; sudo pkill -f \"append_bench\""
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
}

start_discovery() {
    # start discovery
    echo "Starting discovery on sgbhat3@hp${data_0[0]}.utah.cloudlab.us"
    ssh -i $PASSLESS_ENTRY "sgbhat3@hp${data_0[0]}.utah.cloudlab.us" "sh -c \"cd $benchmark_dir/disc; nohup sudo ./run_goreman.sh > /users/sgbhat3/scalog-storage/disc.log 2>&1 &\""
}


start_client() {
    ssh -i $PASSLESS_ENTRY sgbhat3@hp$1.utah.cloudlab.us "cd $benchmark_dir/scripts; sudo ./run_client.sh $2 $3 $1 $4 > client_$1.log 2>&1" &
}


# clients
clients=("1300" "1200" "1000" "900" "800" "700" "600" "512" "256" "128" "64" "30" "25" "20" "18" "16" "12" "10" "8" "6" "4" "2")

for c in "${clients[@]}"; 
do
    for client_node in "${client_nodes[@]}";
    do
        cleanup_client $client_node
    done 

    cleanup_servers

    start_order_nodes
    start_data_nodes 
    start_discovery

    # wait for 10 secs
    sleep 10

    num_client_nodes=${#client_nodes[@]}

    for client_node in "${client_nodes[@]}";
    do
        # run client
        # start_client <client_id> <num_of_clients_to_run> <num_appends_per_client> <total_clients>
        start_client $client_node $(($c/$num_client_nodes)) "2m" $c
    done

    echo "Waiting for clients to terminate"
    wait

    for client_node in "${client_nodes[@]}";
    do
        cleanup_client $client_node
    done

    # clear client logs
    sudo rm -rf *.log
    
    cleanup_servers_wo_log_clear
done