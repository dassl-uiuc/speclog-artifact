#!/bin/bash

remote_nodes=("node0" "node1" "node2" "node3" "node4" "node5" "node6" "node7" "node8" "node9" "node10" "node11" "node12")
PASSLESS_ENTRY="/users/sgbhat3/.ssh/id_rsa"

# Check if the local script path and number of nodes are provided as command line arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <local_script_path> <number_of_nodes>"
    exit 1
fi

local_script="$1"
num_nodes="$2"

# Validate the number of nodes
if ! [[ "$num_nodes" =~ ^[0-9]+$ ]] || [ "$num_nodes" -le 0 ] || [ "$num_nodes" -gt "${#remote_nodes[@]}" ]; then
    echo "Error: <number_of_nodes> must be a positive integer between 1 and ${#remote_nodes[@]}"
    exit 1
fi

# Iterate over the specified number of remote nodes and execute the script
for node in "${remote_nodes[@]:0:num_nodes}"; do
    echo "Executing script on $node..."
    ssh -o StrictHostKeyChecking=no -i ${PASSLESS_ENTRY} sgbhat3@$node "sudo bash -s" < "$local_script" &
done

wait 
echo "Done"