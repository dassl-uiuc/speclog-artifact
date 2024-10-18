#!/bin/bash

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <client_id> <client_number>"
  exit 1
fi

intrusion_detection_dir="/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection"
client_id="$1"
client_number="$2"

# output directory for the test output
output_dir="../results/${interval}/intrusion_detection_append_${total_clients}"
sudo mkdir -p $output_dir

for ((i=0; i<$client_number; i++)); do
    cmd="sudo /usr/local/go/bin/go run $intrusion_detection_dir/intrusion_detection_generator.go $i" 
    $cmd &
done

echo "Waiting for background processes to finish..."
wait