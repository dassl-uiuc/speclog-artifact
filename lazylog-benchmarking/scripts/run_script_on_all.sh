#!/bin/bash

remote_nodes=("136" "034" "007" "158" "127" "144" "121" "147" "021" "126" "039" "159" "038" "036" "124" "123")
PASSLESS_ENTRY="/users/sgbhat3/.ssh/id_rsa"

# Check if the local script path is provided as a command line argument
if [ $# -eq 0 ]; then
    echo "Usage: $0 <local_script_path>"
    exit 1
fi

local_script="$1"

# Iterate over remote nodes and execute the script
for node in "${remote_nodes[@]}"; do
    echo "Executing script on $node..."
    ssh -o StrictHostKeyChecking=no -i ${PASSLESS_ENTRY} "sgbhat3@hp$node.utah.cloudlab.us" "sudo bash -s" < "$local_script" &
done

wait
echo "Done"