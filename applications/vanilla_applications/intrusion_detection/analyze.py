append_throughput_file_path = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/data/append_throughput_"
e2e_latencies_file_path = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/data/e2e_latencies_"
read_throughput_file_path = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/data/read_throughput_"
append_records_produced_file_path = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/data/append_records_produced_"
records_received_file_path = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/data/records_received_"
stats_file_path = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/analytics/stats.txt"

num_replicas = 4
num_append_clients_per_replica = 50
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

    e2e_latency = 0
    line_count = 0
    for i in range(num_replicas):
        for j in range(num_read_clients_per_replica):
            e2e_latencies_file_path_i = e2e_latencies_file_path + str(i) + "_" + str(j) + ".txt"
            with open(e2e_latencies_file_path_i, 'r') as file:
                for line in file:
                    e2e_latency += float(line)
                    line_count += 1

    avg_e2e_latency = e2e_latency / line_count

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

    with open(stats_file_path, 'w') as file:
        file.write("e2e_latency in microseconds: " + str(avg_e2e_latency) + "\n")
        file.write("append_throughput (ops/s): " + str(append_throughput) + "\n")
        file.write("read_throughput (ops/s): " + str(read_throughput) + "\n")
        file.write("records_produced: " + str(records_produced) + "\n")
        file.write("records_consumed: " + str(records_consumed) + "\n")

analyze()