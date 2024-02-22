#!/bin/bash

# Check if the number of clients is provided as an argument
if [ $# -ne 1 ]; then
  echo "Usage: $0 <num_clients>"
  exit 1
fi

# Number of clients
num_clients=$1

# Output Procfile
procfile="Procfile"

# Output directory for the test output
output_dir="../append_bench_$1"
sudo mkdir $output_dir

# Function to generate command for each client
generate_command() {
  client_number=$1
  echo "client-${client_number}: sh -c \"./run_client.sh $output_dir ${client_number}\""
}

# Generate Procfile
echo "# Procfile for ${num_clients} clients" > ${procfile}
for ((i=1; i<=num_clients; i++)); do
  generate_command $i >> ${procfile}
done

echo "Procfile generated successfully."