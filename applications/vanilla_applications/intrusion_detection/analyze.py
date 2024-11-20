append_throughput_file_path = "data/append_throughput_"
append_start_timestamps_file_path = "data/append_start_timestamps_"
compute_e2e_end_times_file_path = "data/compute_e2e_end_times_"
delivery_latencies_file_path = "data/delivery_latencies_"
confirm_latencies_file_path = "data/confirm_latencies_"
read_throughput_file_path = "data/read_throughput_"
append_records_produced_file_path = "data/append_records_produced_"
records_received_file_path = "data/records_received_"
stats_file_path = "analytics/stats.txt"

num_replicas = 2
num_append_clients_per_replica = 10
num_read_clients_per_replica = 1
def analyze():
    records_produced = 0
    for i in range(num_replicas):
        for j in range(num_append_clients_per_replica):
            append_records_produced_file_path_i = append_records_produced_file_path + str(i) + "_" + str(j) + ".txt"
            with open(append_records_produced_file_path_i, 'r') as file:
                for line in file:
                    records_produced += int(line)

    records_consumed = 0
    for i in range(num_replicas):
        for j in range(num_read_clients_per_replica):
            records_received_file_path_i = records_received_file_path + str(i) + "_" + str(j) + ".txt"
            with open(records_received_file_path_i, 'r') as file:
                for line in file:
                    records_consumed += int(line)

    append_throughput = 0
    for i in range(num_replicas):
        for j in range(num_append_clients_per_replica):
            append_throughput_file_path_i = append_throughput_file_path + str(i) + "_" + str(j) + ".txt"
            with open(append_throughput_file_path_i, "r") as append_num_ops_file:
                for line in append_num_ops_file:
                    append_throughput += float(line)

    read_throughput = 0
    for i in range(num_replicas):
        for j in range(num_read_clients_per_replica):
            read_throughput_file_path_i = read_throughput_file_path + str(i) + "_" + str(j) + ".txt"
            with open(read_throughput_file_path_i, 'r') as file:
                for line in file:
                    read_throughput += float(line)

    # Start calculating the latencies
    append_start_timestamps = {}
    num_append_timestamps = 0
    for i in range(num_replicas):
        for j in range(num_append_clients_per_replica):
            append_start_timestamps_file_path_i = f"{append_start_timestamps_file_path}{i}_{j}.txt"
            with open(append_start_timestamps_file_path_i, 'r') as file:
                for line in file:
                    gsn, timestamp = line.strip().split(",")
                    append_start_timestamps[int(gsn)] = int(timestamp)
                    num_append_timestamps += 1

    compute_e2e_latency = 0
    line_count = 0
    for i in range(num_replicas):
        for j in range(num_read_clients_per_replica):
            compute_e2e_end_times_file_path_i = f"{compute_e2e_end_times_file_path}{i}_{j}.txt"
            with open(compute_e2e_end_times_file_path_i, 'r') as file:
                for line in file:
                    gsn, timestamp = line.strip().split(",")
                    compute_e2e_latency += int(timestamp) - append_start_timestamps[int(gsn)]
                    line_count += 1

    avg_compute_e2e_latency = compute_e2e_latency / line_count / 1000

    delivery_e2e_latencies_map = {}
    num_delivery_e2e_latencies = 0
    for i in range(num_replicas):
        for j in range(num_read_clients_per_replica):
            delivery_latencies_file_path_i = f"{delivery_latencies_file_path}{i}_{j}.txt"
            with open(delivery_latencies_file_path_i, 'r') as file:
                for line in file:
                    gsn, timestamp = line.strip().split(",")
                    delivery_e2e_latencies_map[int(gsn)] = int(timestamp)
                    num_delivery_e2e_latencies += 1

    confirm_e2e_latencies_map = {}
    for i in range(num_replicas):
        for j in range(num_read_clients_per_replica):
            confirm_latencies_file_path_i = f"{confirm_latencies_file_path}{i}_{j}.txt"
            with open(confirm_latencies_file_path_i, 'r') as file:
                for line in file:
                    gsn, timestamp = line.strip().split(",")
                    confirm_e2e_latencies_map[int(gsn)] = int(timestamp)
    
    confirm_e2e_latencies = 0
    num_confirm_e2e_latencies = 0
    delivery_e2e_latencies = 0
    num_delivery_e2e_latencies = 0
    for gsn, timestamp in append_start_timestamps.items():
        if gsn in delivery_e2e_latencies_map and gsn in confirm_e2e_latencies_map:
            delivery_e2e_latencies += delivery_e2e_latencies_map[gsn] - timestamp
            confirm_e2e_latencies += confirm_e2e_latencies_map[gsn] - timestamp
            num_delivery_e2e_latencies += 1
            num_confirm_e2e_latencies += 1

    avg_delivery_e2e_latency = delivery_e2e_latencies / num_delivery_e2e_latencies / 1000
    avg_confirm_e2e_latency = confirm_e2e_latencies / num_confirm_e2e_latencies / 1000
    avg_total_e2e_latency = max(avg_compute_e2e_latency, avg_confirm_e2e_latency)

    with open(stats_file_path, 'w') as file:
        file.write("compute_e2e_latency in microseconds: " + str(avg_compute_e2e_latency) + "\n")
        file.write("shared_log_e2e_latency in microseconds: " + str(avg_delivery_e2e_latency) + "\n")
        file.write("confirm_e2e_latency in microseconds: " + str(avg_confirm_e2e_latency) + "\n")
        file.write("total_e2e_latency in microseconds: " + str(avg_total_e2e_latency) + "\n")
        file.write("append_throughput (ops/s): " + str(append_throughput) + "\n")
        file.write("read_throughput (ops/s): " + str(read_throughput) + "\n")
        file.write("records_produced: " + str(records_produced) + "\n")
        file.write("records_consumed: " + str(records_consumed) + "\n")
        file.write("num append timestamps: " + str(num_append_timestamps) + "\n")
        file.write("num delivery e2e latencies: " + str(num_delivery_e2e_latencies) + "\n")

analyze()