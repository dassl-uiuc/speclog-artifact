#!/bin/bash

if [ "$#" -ne 8 ]; then
  echo "Usage: $0 <client_number> <time_limit> <client_id> <total_clients> <interval> <start_sharding_hint> <append_mode> <rate>"
  exit 1
fi

client_number="$1"
time_limit="$2"
client_id="$3"
total_clients="$4"
interval="$5"
start_sharding_hint="$6"
append_mode="$7"
rate="$8"

# output directory for the test output
output_dir="../results/${interval}/append_bench_${total_clients}"
mkdir -p $output_dir

for ((i=1; i<=$client_number; i++)); do
    request_size="4096"
    sharding_hint=$(($start_sharding_hint + $i-1))
    cmd="/usr/local/go/bin/go run append_bench.go $time_limit $request_size $sharding_hint $append_mode $rate ${output_dir}/<hp${client_id}>_${time_limit}_${request_size}_${i}.csv"
    $cmd &
done

echo "Waiting for background processes to finish..."
wait