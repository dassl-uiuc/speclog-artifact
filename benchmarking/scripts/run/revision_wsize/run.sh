#!/bin/bash

source ../../common.sh
pushd $benchmark_dir/scripts

# parameters
runtime_secs=120
computation_time=(1500)
window_size=(5 10 20 50 100 200 300)
num_shards=1

for ct in "${computation_time[@]}";
do 
    for ws in "${window_size[@]}";
    do
        sed -i "s/s.numLocalCutsThreshold = 100/s.numLocalCutsThreshold = ${ws}/" ../../data/data_server.go
        sed -i "s/s.localCutChangeWindow = 100/s.localCutChangeWindow = ${ws}/" ../../order/order_server.go

        pushd $benchmark_dir/../
        go build
        popd

        # wait for NFS to sync
        sleep 5 
        shas=$(./run_script_on_servers.sh ./check_sync.sh $run_server_suffix)
        check_sync $shas

        cleanup_clients
        cleanup_servers
        clear_server_logs
        clear_client_logs

        start_order_nodes
        start_discovery
        start_data_nodes $num_shards

        sleep 1
        start_e2e_clients ${client_nodes[0]} $ct $runtime_secs 0 10 $benchmark_dir/logs/
        start_e2e_clients ${client_nodes[1]} $ct $runtime_secs 1 10 $benchmark_dir/logs/
        echo "Waiting for clients to terminate"

        wait 

        cleanup_clients
        cleanup_servers
        collect_logs $num_shards

        # move logs to a different folder
        mkdir -p "$results_dir/window_size/${ws}/e2e_${ct}"
        mv $benchmark_dir/logs/* "$results_dir/window_size/${ws}/e2e_${ct}"

        # restore the original values
        sed -i "s/s.numLocalCutsThreshold = ${ws}/s.numLocalCutsThreshold = 100/" ../../data/data_server.go
        sed -i "s/s.localCutChangeWindow = ${ws}/s.localCutChangeWindow = 100/" ../../order/order_server.go
    done
done


pushd $benchmark_dir/../
go build
popd

sleep 5
shas=$(./run_script_on_servers.sh ./check_sync.sh $run_server_suffix)
check_sync $shas


popd

