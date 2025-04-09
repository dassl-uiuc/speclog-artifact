import os
import csv
import numpy as np 
import pandas as pd

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

def is_int_convertible(s):
    try:
        int(s)
        return True
    except (ValueError, TypeError):
        return False

def get_latencies(directory):
    all_latencies = []

    for filename in os.listdir(directory):
        if filename.endswith(".csv") and filename[0] == "<":
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r') as csvfile:
                csv_reader = csv.DictReader(csvfile)
                for row in csv_reader:
                    latency_value = row.get("latency(ns)")
                    if latency_value is not None and is_int_convertible(latency_value):
                        all_latencies.append(int(latency_value))
                    else:
                        print(f"Invalid or missing latency value in row: {row}")
    
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
                if first_line[5] is None or first_line[6] is None or not is_int_convertible(first_line[5]) or not is_int_convertible(first_line[6]):
                    print(f"Invalid or missing value in first line of file {filename}: {first_line}")
                else:
                    total_bytes += int(first_line[5])
                    max_total_time = max(max_total_time, int(first_line[6]))
    
    if max_total_time > 0:
        return total_bytes * 1e9 / max_total_time / num_bytes_per_op
    
    return None


root_path="/users/sgbhat3/append/"

df_scalog = pd.DataFrame(columns=["mean", "stddev"])
df_speclog = pd.DataFrame(columns=["mean", "stddev"])
tputs = [20, 40, 100]

for tput in tputs:
    scalog_path = root_path + "append_scalog/"
    for i in [1, 2, 3]:
        scalog_path = root_path + "append_scalog/" + str(i) + "/1ms/append_bench_" + str(tput)
        latencies = get_latencies(scalog_path)
        mean, p50, p99 = get_latency_metrics(latencies)
        df_scalog.loc[tput] = [mean, p99]


