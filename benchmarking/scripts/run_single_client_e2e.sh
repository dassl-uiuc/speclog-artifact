#!/bin/bash

source ./common.sh

# parameters
runtime_secs=20
computation_time=(1200)


for computation_time in "${computation_time[@]}";
do 
    cleanup_clients
    cleanup_servers
    clear_server_logs
    clear_client_logs

    start_order_nodes
    start_data_nodes 
    start_discovery

    start_e2e_clients ${client_nodes[0]} $computation_time $runtime_secs
    echo "Waiting for clients to terminate"

    wait 

    cleanup_clients
    cleanup_servers
    collect_logs
done

