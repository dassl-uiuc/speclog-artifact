#!/usr/bin/python3
## Find burst start lcn and wn
import re
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import sys
import glob
import pandas as pd


def extract_burst_cut(log_file_path):    
    pattern = r"burst local cut number (\d+)"
    with open(log_file_path, "r") as file:
        log_content = file.read()
    matches = re.findall(pattern, log_content)

    if matches:
        return matches[0]
    else:
        return None

# CONFIGURE PATH HERE
logfile = "PATH/scalog_qc/data-0-0.log"
burst_cut_num = extract_burst_cut(logfile)
print(f"Burst cut number: {burst_cut_num}")


# Helper function to parse timestamps
def parse_timestamp(ts):
    return datetime.strptime(ts, "%H:%M:%S.%f")

# File path
log_file = "PATH/scalog_qc/order-0.log"

# Regex patterns
tput_pattern = r"\[real-time tput\]: (\d+) ops/sec"
total_tput_pattern = r"\[real-time total tput\]: (\d+) ops/sec"
timestamp_pattern = r"(\d{2}:\d{2}:\d{2}\.\d{6})"
cut_pattern = rf"{timestamp_pattern} cut:(\d+) cut:(\d+)"
burst_cut_pattern = rf"cut:(\d+) cut:0 localCutNum:{burst_cut_num}"

timestamps = []
tput_values = []
total_tput_timestamps = []
total_tput_values = []
shard_0_cuts = []
shard_1_cuts = []
burst_start = []

with open(log_file, "r") as f:
    for line in f:
        # Extract throughput
        tput_match = re.search(tput_pattern, line)
        total_tput_match = re.search(total_tput_pattern, line)
        timestamp_match = re.search(timestamp_pattern, line)
        
        if tput_match and timestamp_match:
            tput_values.append(int(tput_match.group(1)))
            timestamps.append(parse_timestamp(timestamp_match.group(1)))

        if total_tput_match and timestamp_match:
            total_tput_values.append(int(total_tput_match.group(1)))
            total_tput_timestamps.append(parse_timestamp(timestamp_match.group(1)))

        # Extract shard cuts
        cut_match = re.search(cut_pattern, line)
        if cut_match:
            timestamp, cut_0, cut_1 = cut_match.groups()
            shard_0_cuts.append((parse_timestamp(timestamp), int(cut_0)))
            shard_1_cuts.append((parse_timestamp(timestamp), int(cut_1)))

        # Extract burst start
        burst_cut_pattern_match = re.search(burst_cut_pattern, line)
        if burst_cut_pattern_match and timestamp_match:
            burst_start.append(parse_timestamp(timestamp))


if not burst_start:
    print("No burst start found.")
    exit()
burst_start_time = burst_start[0]

# CONFIGURE ZOOM WINDOW HERE
zoom_window_ms = 10000
zoom_start = burst_start_time - timedelta(milliseconds=zoom_window_ms)
zoom_end = burst_start_time + timedelta(milliseconds=zoom_window_ms)

# Convert to relative times
min_timestamp = min(timestamps + total_tput_timestamps + [c[0] for c in shard_0_cuts + shard_1_cuts])

def to_relative_ms(t):
    return (t - min_timestamp).total_seconds() * 1000

shard_0_times_rel = [to_relative_ms(t) for t, _ in shard_0_cuts if zoom_start <= t <= zoom_end]
shard_1_times_rel = [to_relative_ms(t) for t, _ in shard_1_cuts if zoom_start <= t <= zoom_end]
# quota_times_rel = {to_relative_ms(t): v for t, v in quota_changes.items() if zoom_start <= t <= zoom_end}

burst_start_rel = to_relative_ms(burst_start_time)

## analyze latencies for shard 0


def parse_timestamp(timestamp):
    return datetime.strptime(timestamp, "%H:%M:%S.%f")

def get_lat_ts(path):
    file_pattern = path + "e2e_metrics.csv"
    append_pattern = path + "append_metrics.csv"
    e2e_latency_values = {}
    append_latency_values = {}

    for file in glob.glob(append_pattern):
        with open(file, 'r') as f:
            lines = f.readlines()[1:]  # Skip the header
            for line in lines:
                parts = line.strip().split(',')
                gsn, latency, timestamp, tput = int(parts[0]), int(parts[1]), parse_timestamp(parts[2]), float(parts[3])
                append_latency_values[gsn] = [latency, timestamp]

    for file in glob.glob(file_pattern):
        with open(file, 'r') as f:
            lines = f.readlines()[1:]  # Skip the header
            for line in lines:
                parts = line.strip().split(',')
                gsn, e2e_latency, delivery_timestamp = int(parts[0]), int(parts[2]), parse_timestamp(parts[4])
                e2e_latency_values[gsn] = [e2e_latency, delivery_timestamp]

    with open(path + "client_node13.log", 'r') as f:
        lines = f.readlines()
        for line in lines:
            if "first append start time" in line:
                burst_start = parse_timestamp(line.split()[-1])
                break

    return append_latency_values, e2e_latency_values, burst_start

# CONFIGURE PATH HERE
path = "PATH/scalog_qc/"
append_latency_values, e2e_latency_values, burst_start = get_lat_ts(path)
print(f"burst start: {burst_start}")


### E2E Latency plots

e2e_latency_times = []
e2e_latencies = []
for gsn, (latency, timestamp) in e2e_latency_values.items():
    e2e_latency_times.append(timestamp)
    e2e_latencies.append(latency)

df = pd.DataFrame({
    'time': e2e_latency_times,
    'latency': e2e_latencies
})

df = df.sort_values(by='time')

# CONFIGURE WINDOW SIZE HERE
window_size = 1000  # Set the window size for the moving average
df['moving_avg'] = df['latency'].rolling(window=window_size, min_periods=1).mean()

min_time = df['time'].min()
df['relative_time_ms'] = (df['time'] - min_time).dt.total_seconds() * 1000

# CONFIGURE ZOOM IN PERIOD HERE
start_time = burst_start - timedelta(milliseconds=10000)  
end_time = burst_start + timedelta(milliseconds=10000)  

df_zoomed = df[(df['time'] >= start_time) & (df['time'] <= end_time)]

assert len(df_zoomed['relative_time_ms']) == len(df_zoomed['moving_avg'])

with open("scalog-e2elat", 'w') as f:
    for key in df_zoomed['relative_time_ms'].keys():
        f.write(str(df_zoomed['relative_time_ms'][key]) + "\t" + str(df_zoomed['moving_avg'][key]) + "\n")

#plt.figure(figsize=(10, 6))
#plt.plot(df_zoomed['relative_time_ms'], df_zoomed['moving_avg'], label=f'Moving Average (window={window_size})', color='blue', linewidth=2)

start_time_relative = (burst_start - min_time).total_seconds() * 1000
#plt.axvline(x=start_time_relative, color='green', linestyle=':', label=f'burst start time')

#plt.xlabel('Time (ms)')
#plt.ylabel('Latency (us)')
#plt.title('e2e Latency Over Time with Moving Average')
#plt.xticks(rotation=45)
#plt.ylim(ymin=0, ymax=6000)
#plt.legend()
#plt.grid(True)
#plt.tight_layout()
#plt.savefig("{0}_e2elat.png".format(mode), dpi=600)
