#!/bin/bash

source ../../common.sh

pushd $benchmark_dir/scripts

# change params in files
set_bool_variable_in_file \
    ../../data/data_server.go \
    "qcExpt" \
    "true"

set_bool_variable_in_file \
    ../../order/order_server.go \
    "qcExpt" \
    "true"

set_bool_variable_in_file \
    ../../data/data_server.go \
    "lagfixEnabled" \
    "true"

set_bool_variable_in_file \
    ../../order/order_server.go \
    "lagfixEnabled" \
    "true"


# run with qc enabled first 
pushd $benchmark_dir/../
go build 
popd 

sleep 5
echo "Running with qc enabled"
pushd $benchmark_dir/scripts
# one shard involved in this expt
num_shards=1

## uncomment relavent code in data_server and order_server


# new client joins when?
new_client_join_time=30 
computation_time=(800)

# parameters
runtime_secs=60

cleanup_clients
cleanup_servers
clear_server_logs
clear_client_logs

start_order_nodes
start_discovery

# start shards
start_data_nodes $num_shards

# sleep for a bit befor starting clients
sleep 5

# start clients
start_qc_e2e_clients ${client_nodes[0]} $runtime_secs 5 $new_client_join_time $computation_time $benchmark_dir/logs/ 
echo "Waiting for clients to terminate"

wait 

cleanup_clients
cleanup_servers

collect_logs $num_shards

# move logs to a different folder
mkdir -p "$results_dir/qc_enabled_e2e"
mv $benchmark_dir/logs/* "$results_dir/qc_enabled_e2e"


popd


# run with qc disabled
set_bool_variable_in_file \
    ../../order/order_server.go \
    "qcEnabled" \
    "false"

set_bool_variable_in_file \
    ../../data/data_server.go \
    "qcEnabled" \
    "false"

pushd $benchmark_dir/../
go build
popd

sleep 5 
echo "Running with qc disabled"

pushd $benchmark_dir/scripts
# one shard involved in this expt
num_shards=1

## uncomment relavent code in data_server and order_server


# new client joins when?
new_client_join_time=30 
computation_time=(800)

# parameters
runtime_secs=60

cleanup_clients
cleanup_servers
clear_server_logs
clear_client_logs

start_order_nodes
start_discovery

# start shards
start_data_nodes $num_shards

# sleep for a bit befor starting clients
sleep 5

# start clients
start_qc_e2e_clients ${client_nodes[0]} $runtime_secs 5 $new_client_join_time $computation_time $benchmark_dir/logs/ 
echo "Waiting for clients to terminate"

wait 

cleanup_clients
cleanup_servers

collect_logs $num_shards

# move logs to a different folder
mkdir -p "$results_dir/qc_disabled_e2e"
mv $benchmark_dir/logs/* "$results_dir/qc_disabled_e2e"


popd


# restore defaults and build

set_bool_variable_in_file \
    ../../data/data_server.go \
    "qcExpt" \
    "false"

set_bool_variable_in_file \
    ../../order/order_server.go \
    "qcExpt" \
    "false"

set_bool_variable_in_file \
    ../../data/data_server.go \
    "qcEnabled" \
    "true"

set_bool_variable_in_file \
    ../../order/order_server.go \
    "qcEnabled" \
    "true"

pushd $benchmark_dir/../
go build
popd

popd