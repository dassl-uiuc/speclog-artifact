#!/bin/bash

# Number of shards
n=$1

# Base IPs for data machines
ips=("10.10.1.4" "10.10.1.5" "10.10.1.6" "10.10.1.7" "10.10.1.8" "10.10.1.9" "10.10.1.10" "10.10.1.11" "10.10.1.12" "10.10.1.13")

# Ports
order_port=26734
raft_port=27238
data_port=23282
disc_port=23472

# Node names (nodeX corresponds to 10.10.1.(X+1))
nodes=("node3" "node4" "node5" "node6" "node7" "node8" "node9" "node10" "node11" "node12")

# Generate data_pri and data_sec arrays
data_pri=()
data_sec=()

for ((j=0; j<n; j++)); do
  pri_index=$(( (j * 2) % 10 ))
  sec_index=$(( (j * 2 + 1) % 10 ))
  data_pri+=("\"${nodes[$pri_index]}\"")
  data_sec+=("\"${nodes[$sec_index]}\"")
done

# Output data_pri and data_sec lines
echo "data_pri=(${data_pri[*]})"
echo "data_sec=(${data_sec[*]})"

# Create scalog.yaml
cat <<EOL > ../../.scalog.yaml
order-port: $order_port
raft-port: $raft_port
order-replication-factor: 3
order-batching-interval: 1ms
order-0-ip: "10.10.1.1"
order-1-ip: "10.10.1.2"
order-2-ip: "10.10.1.3"

data-port: $data_port
data-replication-factor: 2
data-batching-interval: 1ms
disc-port: $disc_port
disc-ip: "10.10.1.4"
EOL

# Add IP assignments for each shard
for ((j=0; j<n; j++)); do
  for ((i=0; i<2; i++)); do
    ip_index=$(( (j * 2 + i) % 10 ))
    echo "data-$j-$i-ip: \"${ips[$ip_index]}\"" >> ../../.scalog.yaml
  done
done

# Generate directories and files
for ((j=0; j<n; j++)); do
  for ((i=0; i<2; i++)); do
    dir="../data-$j-$i"
    mkdir -p $dir

    # Create run_goreman.sh
    cat <<EOL > $dir/run_goreman.sh
#!/bin/bash

export PATH=\$PATH:/users/sgbhat3/go/bin/
goreman start
EOL
    chmod +x $dir/run_goreman.sh

    # Create Procfile
    cat <<EOL > $dir/Procfile
# Use goreman to run \`go get github.com/mattn/goreman\`

data-$j-$i: ../../scalog data --config=../../.scalog.yaml --sid=$j --rid=$i
EOL
  done
done

echo "Configuration generation complete for $n shards."
