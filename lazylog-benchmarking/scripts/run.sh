#!/bin/bash
PASSLESS_ENTRY="/users/sgbhat3/.ssh/id_rsa"

# remote_nodes=("034" "078" "097" "007" "090" "030" "096" "116")
# ips=("3" "4" "2" "1" "5" "6" "7" "8")

benchmark_dir="/proj/rasl-PG0/sgbhat3/scalog/lazylog-benchmarking"

# index into remote_nodes/ips for order nodes
order=("007" "090" "030")

# index into remote_nodes/ips for data shards
data_0=("096" "116")

#clients=("18" "16" "12" "10" "8" "6" "4" "2" "1")
clients=("16" "20")
for c in "${clients[@]}"; 
do
    # kill existing servers
    sudo ./run_script_on_all.sh ./kill_all_goreman.sh

    # mount storage and clear existing logs if any
    sudo ./run_script_on_all.sh ./setup_disk.sh

    # start order nodes
    for ((i=0; i<=2; i++))
    do
        echo "Starting order-${i} on sgbhat3@hp${order[$i]}.utah.cloudlab.us"
        ssh -i $PASSLESS_ENTRY "sgbhat3@hp${order[$i]}.utah.cloudlab.us" "sh -c \"cd $benchmark_dir/order-$i; nohup sudo ./run_goreman.sh > /users/sgbhat3/scalog-storage/order-$i.log 2>&1 &\""
    done

    # start data nodes
    for ((i=0; i<=1; i++))
    do
        echo "Starting data-0-${i} on sgbhat3@hp${data_0[$i]}.utah.cloudlab.us"
        ssh -i $PASSLESS_ENTRY "sgbhat3@hp${data_0[$i]}.utah.cloudlab.us" "sh -c \"cd $benchmark_dir/data-0-$i; nohup sudo ./run_goreman.sh > /users/sgbhat3/scalog-storage/data-0-$i.log 2>&1 &\""
    done

    # start discovery
    echo "Starting discovery on sgbhat3@hp${data_0[0]}.utah.cloudlab.us"
    ssh -i $PASSLESS_ENTRY "sgbhat3@hp${data_0[0]}.utah.cloudlab.us" "sh -c \"cd $benchmark_dir/disc; nohup sudo ./run_goreman.sh > /users/sgbhat3/scalog-storage/disc.log 2>&1 &\""


    # wait for 5 secs
    sleep 5

    # run clients
    ./run_client.sh $c
done