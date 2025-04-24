#!/usr/bin/env python3

import numpy as np
import glob
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timedelta
import sys

# analyze logs to get latency metrics for lagfix experiment
def get_latency_metrics_for_lagfix(path):
    e2e_file_pattern = path + "e2e_metrics.csv"
    append_file_pattern = path + "append_latency_timestamp.csv"
    append_latency_values = {}
    e2e_latency_values = {}
    gsns = []

    for file in glob.glob(e2e_file_pattern):
        with open(file, 'r') as f:
            lines = f.readlines()[1:]  # Skip the header
            for line in lines:
                parts = line.strip().split(',')
                gsn, e2e_latency, delivery_timestamp = int(parts[0]), int(parts[2]), parts[4]
                e2e_latency_values[gsn] = [e2e_latency, delivery_timestamp]


    for file in glob.glob(append_file_pattern):
        with open(file, 'r') as f:
            lines = f.readlines()[1:]  # Skip the header
            for line in lines:
                parts = line.strip().split(',')
                gsn, timestamp, latency = int(parts[0]), parts[1], int(parts[2])
                append_latency_values[gsn] = [latency, timestamp]
    
    client_file = path + "client_node13.log"
    with open(client_file, 'r') as f:
        log_data = f.read()
        for line in log_data.splitlines():
            if "gsn: " in line:
                try:
                    gsn = int(line.split()[-1])  
                    gsns.append(gsn)
                except ValueError:
                    print(f"Skipping invalid GSN: {line.split()[-1]}")

    gsns.sort()
    return append_latency_values, e2e_latency_values, gsns


path = "../../../results/scalog_lagfix/"
append_latency_values, e2e_latency_values, gsns = get_latency_metrics_for_lagfix(path)

append_latency_array = np.array([append_latency_values[gsn][0] for gsn in append_latency_values])
mean_latency = np.mean(append_latency_array)
p99_latency = np.percentile(append_latency_array, 99)

e2e_latency_array = np.array([e2e_latency_values[gsn][0] for gsn in e2e_latency_values])
mean_e2e_latency = np.mean(e2e_latency_array)
p99_e2e_latency = np.percentile(e2e_latency_array, 99)

print(f"mean append latency: {mean_latency:.2f} us")
print(f"p99 latency: {p99_latency:.2f} us")
print(f"mean e2e latency: {mean_e2e_latency:.2f} us")
print(f"p99 e2e latency: {p99_e2e_latency:.2f} us")

## Plot e2e latency over time with moving average


e2e_latency_times = []
e2e_latencies = []
for gsn, (latency, timestamp) in e2e_latency_values.items():
    e2e_latency_times.append(datetime.strptime(timestamp, "%H:%M:%S.%f"))
    e2e_latencies.append(latency)

df = pd.DataFrame({
    'time': e2e_latency_times,
    'latency': e2e_latencies
})

df = df.sort_values(by='time')

# CONFIGURE WINDOW SIZE HERE
window_size = 20  # Set the window size for the moving average
df['moving_avg'] = df['latency'].rolling(window=window_size, min_periods=1).mean()

min_time = df['time'].min()
df['relative_time_ms'] = (df['time'] - min_time).dt.total_seconds() * 1000

# CONFIGURE ZOOM IN PERIOD HERE
start_time = datetime.strptime(e2e_latency_values[gsns[0]][1], "%H:%M:%S.%f") - timedelta(milliseconds=100)  
end_time = datetime.strptime(e2e_latency_values[gsns[0]][1], "%H:%M:%S.%f") + timedelta(milliseconds=100)  

df_zoomed = df[(df['time'] >= start_time) & (df['time'] <= end_time)]

start_offset = df_zoomed['relative_time_ms'].iloc[0]
df_zoomed['relative_time_ms'] = df_zoomed['relative_time_ms']-start_offset
df_zoomed[['relative_time_ms', 'moving_avg']].to_csv('scalog-e2e.csv', index=False, sep='\t') 

start_time_relative = (datetime.strptime(e2e_latency_values[gsns[0]][1], "%H:%M:%S.%f") - min_time).total_seconds() * 1000
end_time_relative = (datetime.strptime(e2e_latency_values[gsns[-1]][1], "%H:%M:%S.%f") - min_time).total_seconds() * 1000
print(str(start_offset))
print(str(start_time_relative-start_offset) + "\t" + "burst start time")
print(str(end_time_relative-start_offset) + "\t" + "burst end time")
print(end_time_relative-start_time_relative)
print(end_time-start_time)
