#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <client_number>"
  exit 1
fi

# Command-line arguments
# Output directory for the test output
output_dir="../append_bench_$1"
sudo mkdir $output_dir

client_number="$1"

# Array to store background process IDs
process_ids=()

# Run the Go program for the specified number of clients
for ((i=1; i<=$client_number; i++)); do
# Perform the division
    requests=$(echo "scale=0; 100000 / $client_number" | bc)
    # request="100"
    request_size="100"
    cmd="sudo /usr/local/go/bin/go run append_bench.go $requests $request_size ${output_dir}/${requests}_${request_size}_${i}.csv"
    
    # Run the Go command in the background
    $cmd &
    
    # Store the background process ID
    process_ids+=($!)
    
    echo "Running \"$cmd\" for client $i"
done

# Wait for all background processes to finish
echo "Waiting for background processes to finish..."
for pid in "${process_ids[@]}"; do
    wait "$pid"
    echo "done"
done