#!/usr/bin/env python3
## Analyze latencies for lagfix experiment
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import bisect
import re
import sys
import os 

results_dir = os.getenv("results_dir")

timestamp_pattern = r"(\d{2}:\d{2}:\d{2}\.\d{6})"
jt = float(sys.argv[1])

path = results_dir + "/reconfig_800_scalog_with_e2e/"

def parse_timestamp(time):
    return datetime.strptime(time, "%H:%M:%S.%f")

def analyze_reconfig(path):
    client_path = path + "client_node13.log"
    e2e_path = path + "e2e_metrics.csv"

    join_time = None
    leave_time = None
    # Determine join and leave GSN
    with open(client_path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if "View id: 2" in line and join_time is None:
                join_time = re.search(timestamp_pattern, line).group(1)
            if "View id: 3" in line and leave_time is None:
                leave_time = re.search(timestamp_pattern, line).group(1)

    # Parse e2e metrics
    timestamps = []
    latencies = []
    gsns = []
    with open(e2e_path, 'r') as f:
        lines = f.readlines()[1:]
        for line in lines:
            parts = line.strip().split(',')
            gsn = int(parts[0])
            e2e_latency = int(parts[2])
            timestamp = parts[4]
            gsns.append(gsn)
            latencies.append(e2e_latency)
            timestamps.append(datetime.strptime(timestamp, "%H:%M:%S.%f"))

    # Prepare data for the plot
    df = pd.DataFrame({
        'time': timestamps,
        'latency': latencies
    })
    df = df.sort_values(by='time')

    # Calculate moving average
    window_size = 2000  # Set window size for moving average
    df['moving_avg'] = df['latency'].rolling(window=window_size, min_periods=1).mean()

    # Calculate relative time
    min_time = df['time'].min()
    # Plot join and leave markers
    join_time_relative = (parse_timestamp(join_time) - min_time).total_seconds() * 1000
    leave_time_relative = (parse_timestamp(leave_time) - min_time).total_seconds() * 1000
    df['relative_time_ms'] = (df['time'] - min_time).dt.total_seconds() * 1000

    start_offset = join_time_relative - jt
    print("Start offset: " + str(start_offset))
    df['relative_time_ms'] =  df['relative_time_ms'] - start_offset
    df[['relative_time_ms', 'moving_avg']].to_csv('scalog_lat.csv', index=False, sep='\t') 
    # Plot moving average and markers
    
    print(str(join_time_relative-start_offset) + "\t" + 'Shard Added')
    print(str(leave_time_relative-start_offset) + "\t" + 'Shard Removed')

    # Calculate the average latency
    average_latency = df['latency'].mean()

    print(average_latency)


analyze_reconfig(path)