#!/bin/bash

source ../../common.sh

pushd $benchmark_dir/scripts

# change params in files
set_bool_variable_in_file \
    ../../data/data_server.go \
    "lagfixExpt" \
    "true"

set_bool_variable_in_file \
    ../../order/order_server.go \
    "lagfixExpt" \
    "true"

# run with lagfix enabled first 
pushd $benchmark_dir/../
go build 
popd 

sleep 5
shas=$(./run_script_on_servers.sh ./check_sync.sh $run_server_suffix)
check_sync $shas

pushd $benchmark_dir/scripts

# single shard involved in this expt
num_shards=1

# ensure that quota is fixed to 10 in the data server and order server before starting this experiment i.e. turn off quota change

# parameters
runtime_secs=30
computation_time=(800)

cleanup_clients
cleanup_servers
clear_server_logs
clear_client_logs

start_order_nodes
start_discovery

# start shard 0 with rid 0 and rid 1
start_data_nodes $num_shards

# sleep for a bit befor starting clients
sleep 5

# start clients
start_lagfix_e2e_clients ${client_nodes[0]} $runtime_secs 3 $computation_time $benchmark_dir/logs/
echo "Waiting for clients to terminate"

wait 

cleanup_clients
cleanup_servers

collect_logs $num_shards

# move logs to a different folder
mkdir -p "$results_dir/scalog_lagfix"
mv $benchmark_dir/logs/* "$results_dir/scalog_lagfix"


popd

# restore defaults and build

set_bool_variable_in_file \
    ../../data/data_server.go \
    "lagfixExpt" \
    "false"

set_bool_variable_in_file \
    ../../order/order_server.go \
    "lagfixExpt" \
    "false"

pushd $benchmark_dir/../
go build
popd

sleep 5
shas=$(./run_script_on_servers.sh ./check_sync.sh $run_server_suffix)
check_sync $shas

popd