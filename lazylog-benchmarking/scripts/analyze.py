import os
import csv

def calculate_avg_throughput_latency(directory, num_bytes_per_op):
    total_bytes_sum = 0
    max_total_time = 0
    total_time_sum = 0
    num_ops_total = 0

    if os.path.exists(directory):
        for filename in os.listdir(directory):
            if filename.endswith(".csv"):
                file_path = os.path.join(directory, filename)

                with open(file_path, 'r') as csvfile:
                    csv_reader = csv.reader(csvfile)
                    header = next(csv_reader)
                    first_line = next(csv_reader, None)

                if first_line:
                    total_bytes = int(first_line[5])
                    total_time = int(first_line[6])

                    total_bytes_sum += total_bytes
                    max_total_time = max(max_total_time, total_time)
                    total_time_sum += total_time
                    num_ops_total += total_bytes/num_bytes_per_op

        if max_total_time > 0:
            avg_throughput = num_ops_total / max_total_time * 1e9
            avg_latency = total_time_sum * 1e-6 / num_ops_total
            return (avg_throughput, avg_latency)
        else:
            return (None, None)
    return (None, None)

clients = [2, 4, 6, 8, 10, 12, 16, 18, 20, 24, 30, 64, 128, 256, 512, 600, 700, 800, 900, 1000, 1200, 1300]
throughput = []
latency = []

for n in clients:
    # Specify the directory path
    directory_path = "../append_bench_" + str(n)

    # Calculate and print the average throughput
    average_throughput, average_latency = calculate_avg_throughput_latency(directory_path, 4096)
    throughput.append(average_throughput)
    latency.append(average_latency)
    print(f"{n} clients\n\taverage throughput: {average_throughput} ops/sec\n\taverage latency: {average_latency} msec/op")