#!/bin/bash

remote_nodes=("node13" "node14" "node15" "node12")
PASSLESS_ENTRY="/users/sgbhat3/.ssh/id_rsa"

# Check if the correct number of arguments is provided
if [ $# -ne 2 ]; then
    echo "Usage: $0 <local_script_path> <num_nodes>"
    exit 1
fi

local_script="$1"
num_nodes="$2"

# Validate that num_nodes is a positive integer
if ! [[ "$num_nodes" =~ ^[0-9]+$ ]] || [ "$num_nodes" -le 0 ]; then
    echo "Error: <num_nodes> must be a positive integer."
    exit 1
fi

# Ensure num_nodes does not exceed the number of available remote nodes
if [ "$num_nodes" -gt "${#remote_nodes[@]}" ]; then
    echo "Error: <num_nodes> exceeds the number of available remote nodes (${#remote_nodes[@]})."
    exit 1
fi

# Iterate over the specified number of remote nodes and execute the script
for ((i = 0; i < num_nodes; i++)); do
    node="${remote_nodes[i]}"
    echo "Executing script on $node..."
    ssh -o StrictHostKeyChecking=no -i ${PASSLESS_ENTRY} sgbhat3@$node "sudo bash -s" < "$local_script" &
done

wait
echo "Done"