#!/bin/bash

if [ "$#" -ne 5 ]; then
  echo "Usage: $0 <client_id> <num_replica> <client_number> <append_type> <appender_id>"
  exit 1
fi

client_id="$1"
num_replica="$2"
client_number="$3"
append_type="$4"
appender_id="$5"

echo "Running hft appender on node $client_id with appender id $appender_id"
for ((i=0; i<$client_number; i++)); do
  offset=$(( $appender_id * $client_number + $i ))
  real_appender_id=$(( offset / 5 ))
  cmd="sudo /usr/local/go/bin/go run ../../applications/vanilla_applications/hft/hft_generator.go $real_appender_id $append_type $i $offset 4"
  $cmd &
done

echo "Waiting for background processes to finish..."
wait