import os
import csv
import numpy as np 

# def check_timeouts(directory):
#     for filename in os.listdir(directory):
#         if filename.endswith(".csv"):
#             file_path = os.path.join(directory, filename)
#             with open(file_path, 'r') as csvfile:
#                 csv_reader = csv.reader(csvfile)
#                 header = next(csv_reader)
#                 first_line = next(csv_reader, None)

#             if first_line and int(first_line[7]) != 0:
#                 return True
    
#     return False

def get_latencies(directory):
    all_latencies = []

    for filename in os.listdir(directory):
        if filename.endswith(".csv") and filename[0] == "<":
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r') as csvfile:
                csv_reader = csv.DictReader(csvfile)
                latencies = [int(row["latency(ns)"]) for row in csv_reader]
                all_latencies.extend(latencies)
    
    return all_latencies

def calculate_percentile(latencies, percentile):
    return np.percentile(latencies, percentile)

def get_latency_metrics(latencies):
    return np.mean(latencies)/1e6, calculate_percentile(latencies, 50)/1e6, calculate_percentile(latencies, 99)/1e6

def get_avg_throughput(directory, num_bytes_per_op):
    total_bytes = 0
    max_total_time = 0

    for filename in os.listdir(directory):
        if filename.endswith(".csv") and filename[0] == "<":
            file_path = os.path.join(directory, filename)

            with open(file_path, 'r') as csvfile:
                csv_reader = csv.reader(csvfile)
                header = next(csv_reader)
                first_line = next(csv_reader, None)
            
            if first_line:
                total_bytes += int(first_line[5])
                max_total_time = max(max_total_time, int(first_line[6]))
    
    if max_total_time > 0:
        return total_bytes * 1e9 / max_total_time / num_bytes_per_op
    
    return None

#clients = [2, 4, 6, 8, 16, 20, 32, 64, 128, 256, 512, 600, 700, 800, 900, 1000, 1200, 1300]
clients = [80, 480]
throughput = []
latency = []

print(f"#clients,avg tput(ops/sec),avg latency(ms/op),p50 latency(ms/op),p99 latency(ms/op)")
for n in clients:
    # Specify the directory path
    directory_path = "../results/1ms/append_bench_" + str(n)

    # Calculate and print the average throughput
    avg_tput = get_avg_throughput(directory_path, 4096)
    mean, p50, p99 = get_latency_metrics(get_latencies(directory_path))

    # timeout = check_timeouts(directory_path)
    # if timeout: 
    #     print("warning! timeouts detected in measurements")

    print(f"{n},{avg_tput},{mean},{p50},{p99}")