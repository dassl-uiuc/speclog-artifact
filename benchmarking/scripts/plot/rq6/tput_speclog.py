#!/usr/bin/env python3
import re
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
# File path
log_file = "../../../results/reconfig_800_speclog_with_e2e/order-0.log"

# Regex patterns
tput_pattern = r"\[real-time tput\]: (\d+) ops/sec"
timestamp_pattern = r"(\d{2}:\d{2}:\d{2}\.\d{6})"

# Data storage
timestamps = []
tput_values = []
shard_join_request = []
shard_notified_to_be_added = []
shard_notified_to_be_removed = []
first_cut_committed = []
last_cut_committed = []
shard_leave_request = []

# Parse the log file
with open(log_file, "r") as f:
    for line in f:
        # Extract real-time throughput
        tput_match = re.search(tput_pattern, line)
        timestamp_match = re.search(timestamp_pattern, line)
        
        if tput_match and timestamp_match:
            tput_values.append(int(tput_match.group(1)))
            timestamps.append(datetime.strptime(timestamp_match.group(1), "%H:%M:%S.%f"))
        
        # Extract events for annotation
        if "Shard 1 to be added in next avl window" in line:
            shard_join_request.append(timestamp_match.group(1))

        if shard_notified_to_be_added == [] and "shardQuotas:<key:2" in line:
            shard_notified_to_be_added.append(timestamp_match.group(1))
        
        if first_cut_committed == [] and "cut:<key:2" in line:
            first_cut_committed.append(timestamp_match.group(1))
        
        if last_cut_committed == [] and "Replica 2 finalized" in line:
            last_cut_committed.append(timestamp_match.group(1))

        if "Shard 1 to be finalized in next avl window" in line:
            shard_leave_request.append(timestamp_match.group(1))

        if "Incrementing view ID because shardFinalized: true" in line:
            shard_notified_to_be_removed.append(timestamp_match.group(1))


# Convert timestamps to seconds since the start
start_time = timestamps[0]
time_in_seconds = [(ts - start_time).total_seconds() for ts in timestamps]


df = pd.DataFrame({
    'time': time_in_seconds,
    'tput': tput_values
})

df = df.sort_values(by='time')

start_offset = 10
# CONFIGURE WINDOW SIZE HERE
window_size = 2  # Set the window size for the moving average
df['moving_avg'] = df['tput'].rolling(window=window_size, min_periods=1).mean()
df['time'] = df['time']-start_offset
df[['time', 'moving_avg']].to_csv('speclog_tput.csv', index=False, sep='\t') 

# Plot the data

# Event times
shard_join_request = (datetime.strptime(shard_join_request[0], "%H:%M:%S.%f") - start_time).total_seconds()
shard_notified_to_be_added_time = (datetime.strptime(shard_notified_to_be_added[0], "%H:%M:%S.%f") - start_time).total_seconds()
first_cut_committed_time = (datetime.strptime(first_cut_committed[0], "%H:%M:%S.%f") - start_time).total_seconds()
shard_leave_request_time = (datetime.strptime(shard_leave_request[0], "%H:%M:%S.%f") - start_time).total_seconds()
shard_notified_to_be_removed_time = (datetime.strptime(shard_notified_to_be_removed[0], "%H:%M:%S.%f") - start_time).total_seconds()
last_cut_committed_time = (datetime.strptime(last_cut_committed[0], "%H:%M:%S.%f") - start_time).total_seconds()


# Add vertical lines for events
print(str(shard_join_request-start_offset) + "\t" + "shard requests to join")
print(str(shard_notified_to_be_added_time-start_offset) + "\t" + "shard notified about addition window")
print(str(first_cut_committed_time-start_offset) + "\t" + "first cut committed from new shard")
print(str(shard_leave_request_time-start_offset) + "\t" + "shard requests to leave")
print(str(shard_notified_to_be_removed_time-start_offset) + "\t" + "shard notified about removal window")
print(str(last_cut_committed_time-start_offset) + "\t" + "last cut committed from leaving shard")