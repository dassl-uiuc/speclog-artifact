#!/bin/bash

if [ "$#" -ne 5 ]; then
  echo "Usage: $0 <client_id> <num_replica> <client_number> <append_type> <appender_id>"
  exit 1
fi

intrusion_detection_dir="/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection"
client_id="$1"
num_replica="$2"
client_number="$3"
append_type="$4"
appender_id="$5"

# output directory for the test output
output_dir="../results/${interval}/intrusion_detection_append_${total_clients}"
sudo mkdir -p $output_dir

echo "Running intrusion detection appender on node $client_id with appender id $appender_id"
for ((i=0; i<$client_number; i++)); do
  cmd="sudo /usr/local/go/bin/go run $intrusion_detection_dir/intrusion_detection_generator.go $appender_id $append_type $i"
  $cmd &
done

echo "Waiting for background processes to finish..."
wait