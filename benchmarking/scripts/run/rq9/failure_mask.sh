#!/bin/bash

fail_mask=true
source ../../common.sh
pushd $benchmark_dir/scripts
push_three_way_yaml

set_bool_variable_in_file \
    ../../data/data_server.go \
    "failMaskExpt" \
    "true"

set_bool_variable_in_file \
    ../../storage/storage.go \
    "failMaskExpt" \
    "true"

pushd $benchmark_dir/../
go build
popd 

sleep 5



# parameters
runtime_secs=60
computation_time=(1500)
num_shards=1

for computation_time in "${computation_time[@]}";
do 
    cleanup_clients
    cleanup_servers
    clear_server_logs
    clear_client_logs

    start_order_nodes
    start_discovery
    start_data_nodes $num_shards

    sleep 1
    start_e2e_clients_3way ${client_nodes[0]} $computation_time $runtime_secs 0 15 $benchmark_dir/logs/
    echo "Waiting for clients to terminate"

    wait 

    cleanup_clients
    cleanup_servers
    collect_logs $num_shards

    # move logs to a different folder
    mkdir -p "$results_dir/failure_mask/e2e_${computation_time}"
    mv $benchmark_dir/logs/* "$results_dir/failure_mask/e2e_${computation_time}"
done

set_bool_variable_in_file \
    ../../data/data_server.go \
    "failMaskExpt" \
    "false"

set_bool_variable_in_file \
    ../../storage/storage.go \
    "failMaskExpt" \
    "false"

pushd $benchmark_dir/../
go build
popd 

sleep 5

popd
pop_three_way_yaml