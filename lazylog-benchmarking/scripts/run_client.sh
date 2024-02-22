#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <output_dir> <client_number>"
  exit 1
fi

# Command-line arguments
output_dir="$1"
client_number="$2"

export PATH=$PATH:/usr/local/go/bin
# Run the go command
echo "Running \"go run append_bench.go 10000 100 ${output_dir}/10K_100B_${client_number}.csv\""
sudo /usr/local/go/bin/go run append_bench.go 10000 100 ${output_dir}/10K_100B_${client_number}.csv