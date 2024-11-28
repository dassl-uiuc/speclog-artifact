#!/bin/bash

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <client_id> <num_replica> <client_number> <reader_id>"
  exit 1
fi

client_id="$1"
num_replica="$2"
client_number="$3"
reader_id="$4"

echo "Running transaction analysis reader on node $client_id with reader id $reader_id"
for ((i=0; i<$client_number; i++)); do
  cmd="sudo /usr/local/go/bin/go run ../../applications/vanilla_applications/transaction_analysis/transaction_analysis.go $reader_id $i"
  $cmd &
done

echo "Waiting for background processes to finish..."
wait