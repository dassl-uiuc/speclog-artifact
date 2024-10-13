#!/bin/bash

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <num_clients> <rate>"
  exit 1
fi

toy_script_dir="/proj/rasl-PG0/tshong/speclog/toy_script"
num_clients="$1"
rate="$2"

for ((i=0; i<$num_clients; i++)); do
    cmd="sudo /usr/local/go/bin/go run $toy_script_dir/produce.go $rate" 
    $cmd &
done

echo "Waiting for background processes to finish..."
wait