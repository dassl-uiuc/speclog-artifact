#!/bin/bash

PASSLESS_ENTRY="/users/sgbhat3/.ssh/id_rsa"

# Check if the local script path is provided as a command line argument
if [ $# -eq 0 ]; then
    echo "Usage: $0 <local_script_path>"
    exit 1
fi

local_script="$1"

# Iterate over remote nodes and execute the script
for ((i=0; i<12; i++)); do
    echo "Executing script on node$i..."
    ssh -o StrictHostKeyChecking=no -i ${PASSLESS_ENTRY} sgbhat3@node$i "sudo bash -s" < "$local_script" &
done

wait
echo "Done"