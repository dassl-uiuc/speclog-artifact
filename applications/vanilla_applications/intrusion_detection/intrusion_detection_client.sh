#!/bin/bash

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <client_id> <num_replica> <client_number> <reader_id>"
  exit 1
fi

intrusion_detection_dir="/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection"
client_id="$1"
num_replica="$2"
client_number="$3"
reader_id="$4"

# output directory for the test output
output_dir="../results/${interval}/intrusion_detection_read_${total_clients}"
sudo mkdir -p $output_dir

echo "Running intrusion detection reader on node $client_id with reader id $reader_id"
for ((i=0; i<$client_number; i++)); do
  cmd="sudo /usr/local/go/bin/go run $intrusion_detection_dir/intrusion_detection_devices.go $reader_id $i"
  $cmd &
done

echo "Waiting for background processes to finish..."
wait