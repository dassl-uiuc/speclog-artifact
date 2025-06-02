#!/bin/bash

source ../../common.sh
pushd $benchmark_dir/scripts

# best numbers at sc 4 4 4 8 8 10
# best rates at 10k 11k 11k 11k 11k 11k_60out
staggering_factors=(4 4 4 8 8 10)
num_shards=(6 8 10 12 16 20)
rates=(10000 11000 11000 11000 11000 11000)

for i in "${!staggering_factors[@]}"
do
    num_shard=${num_shards[$i]}
    rate=${rates[$i]}
    staggering_factor=${staggering_factors[$i]}

    echo "Setting staggering factor to $staggering_factor"
    sed -i "s/const staggeringFactor int64 = -1/const staggeringFactor int64 = $staggering_factor/" ../../data/data_server.go
    sed -i "s/const staggeringFactor int64 = -1/const staggeringFactor int64 = $staggering_factor/" ../../order/order_server.go

    if [ $num_shard -eq 20 ]; then
        sed -i "s/s.emulationOutstandingLimit = 50/s.emulationOutstandingLimit = 60/" ../../data/data_server.go
    fi 
    pushd $benchmark_dir/../
    go build
    popd 

    sleep 5 
    shas=$(./run_script_on_servers.sh ./check_sync.sh $run_server_suffix)
    check_sync $shas


    echo "Running emulation for $num_shard shards with rate $rate"
    cleanup_clients
    cleanup_servers
    clear_server_logs
    clear_client_logs

    start_order_nodes
    start_discovery
    start_data_nodes ${num_shard} ${rate}

    sleep 140

    cleanup_clients
    cleanup_servers
    collect_logs ${num_shard}

    # move logs to a different folder
    mkdir -p "$results_dir/speclog_em/emulation_$num_shard"
    mv $benchmark_dir/logs/* "$results_dir/speclog_em/emulation_$num_shard"


    echo "Resetting staggering factor to -1"
    sed -i "s/const staggeringFactor int64 = $staggering_factor/const staggeringFactor int64 = -1/" ../../data/data_server.go
    sed -i "s/const staggeringFactor int64 = $staggering_factor/const staggeringFactor int64 = -1/" ../../order/order_server.go

    if [ $num_shard -eq 20 ]; then
        sed -i "s/s.emulationOutstandingLimit = 60/s.emulationOutstandingLimit = 50/" ../../data/data_server.go
    fi 

    pushd $benchmark_dir/../
    go build
    popd 
done 

popd



