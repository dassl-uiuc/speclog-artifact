append_throughput_file_path = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/analytics/append_throughput.txt"
e2e_latencies_file_path = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/analytics/e2e_latencies.txt"
read_throughput_file_path = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/analytics/read_throughput.txt"
stats_file_path = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/analytics/stats.txt"

def analyze():
    e2e_latency = 0
    line_count = 0
    with open(e2e_latencies_file_path, 'r') as file:
        for line in file:
            e2e_latency += float(line)
            line_count += 1

    avg_e2e_latency = e2e_latency / line_count

    append_throughput = 0
    with open(append_throughput_file_path, 'r') as file:
        for line in file:
            append_throughput += float(line)

    read_throughput = 0
    with open(read_throughput_file_path, 'r') as file:
        for line in file:
            read_throughput += float(line)

    with open(stats_file_path, 'w') as file:
        file.write("e2e_latency in microseconds: " + str(avg_e2e_latency) + "\n")
        file.write("append_throughput (ops/s): " + str(append_throughput) + "\n")
        file.write("read_throughput (ops/s): " + str(read_throughput) + "\n")

analyze()