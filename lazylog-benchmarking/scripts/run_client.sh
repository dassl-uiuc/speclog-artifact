#!/bin/bash

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <client_number> <time_limit> <client_id> <total_clients>"
  exit 1
fi

client_number="$1"
time_limit="$2"
client_id="$3"
total_clients="$4"

# output directory for the test output
output_dir="../append_bench_${total_clients}"
sudo mkdir $output_dir

for ((i=1; i<=$client_number; i++)); do
    request_size="100"
    cmd="sudo /usr/local/go/bin/go run append_bench.go $time_limit $request_size ${output_dir}/<hp${client_id}>_${time_limit}_${request_size}_${i}.csv"
    $cmd &
done

echo "Waiting for background processes to finish..."
wait