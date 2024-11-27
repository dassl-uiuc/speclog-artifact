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

echo "Running transaction analysis appender on node $client_id with appender id $appender_id"
for ((i=0; i<$client_number; i++)); do
  cmd="sudo /usr/local/go/bin/go run ../../applications/vanilla_applications/transaction_analysis/transaction_analysis_generator.go $appender_id $append_type $i"
  $cmd &
done

echo "Waiting for background processes to finish..."
wait