#!/bin/bash

remote_nodes=("node13" "node14" "node15" "node12")

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

# where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Path to the .env file
ENV_FILE="$SCRIPT_DIR/.env"

# Ensure num_nodes does not exceed the number of available remote nodes
if [ "$num_nodes" -gt "${#remote_nodes[@]}" ]; then
    echo "Error: <num_nodes> exceeds the number of available remote nodes (${#remote_nodes[@]})."
    exit 1
fi

# Iterate over the specified number of remote nodes and execute the script
for ((i = 0; i < num_nodes; i++)); do
    node="${remote_nodes[i]}"
    echo "Executing script on $node..."
    ssh -o StrictHostKeyChecking=no -i ${PASSLESS_ENTRY} $node "source $ENV_FILE && sudo bash -s" < "$local_script" &
done

wait
echo "Done"