#!/bin/bash

# Check if the local script path is provided as a command line argument
if [ $# -eq 0 ]; then
    echo "Usage: $0 <local_script_path>"
    exit 1
fi

local_script="$1"

# where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Path to the .env file
ENV_FILE="$SCRIPT_DIR/.env"

# Iterate over remote nodes and execute the script
for ((i=0; i<16; i++)); do
    echo "Executing script on node$i..."
    ssh -o StrictHostKeyChecking=no -i ${PASSLESS_ENTRY} node$i "source $ENV_FILE && sudo bash -s" < "$local_script" &
done

wait
echo "Done"