#!/usr/bin/env python3

import re
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
# File path
log_file = "PATH/reconfig_800_scalog_with_e2e/order-0.log"

# Regex patterns
tput_pattern = r"\[real-time tput\]: (\d+) ops/sec"
timestamp_pattern = r"(\d{2}:\d{2}:\d{2}\.\d{6})"

# Data storage
timestamps = []
tput_values = []
shard_added = [] # when did the shards send their first cut to the OL
first_cut_committed = [] # when did the first cut get committed
shard_leave_request = [] 
shard_finalized = []
replica_2_added = []
replica_3_added = []
replica_2_committed = []
replica_3_committed = []

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
        if replica_2_added == [] and "Replica 2 added" in line:
            replica_2_added.append(timestamp_match.group(1))

        if replica_3_added == [] and "Replica 3 added" in line:
            replica_3_added.append(timestamp_match.group(1))

        if replica_2_committed == [] and "cut:<key:2" in line:
            replica_2_committed.append(timestamp_match.group(1))
        
        if replica_3_committed == [] and "cut:<key:3" in line:
            replica_3_committed.append(timestamp_match.group(1))
        
        if shard_finalized == [] and "finalizeShards:<shardIDs:1 >" in line:
            shard_finalized.append(timestamp_match.group(1))

        if shard_leave_request == [] and "Shard 1 to be finalized" in line:
            shard_leave_request.append(timestamp_match.group(1))

shard_added.append(max(replica_2_added[0], replica_3_added[0]))
first_cut_committed.append(max(replica_2_committed[0], replica_3_committed[0]))

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
df[['time', 'moving_avg']].to_csv('scalog_tput.csv', index=False, sep='\t') 


# Event times
shard_join_request = (datetime.strptime(shard_added[0], "%H:%M:%S.%f") - start_time).total_seconds()
first_cut_committed_time = (datetime.strptime(first_cut_committed[0], "%H:%M:%S.%f") - start_time).total_seconds()
shard_leave_request_time = (datetime.strptime(shard_leave_request[0], "%H:%M:%S.%f") - start_time).total_seconds()
shard_finalized_time = (datetime.strptime(shard_finalized[0], "%H:%M:%S.%f") - start_time).total_seconds()

# Add vertical lines for events
print(str(shard_join_request - start_offset) + "\t" + "shard requests to join")
print(str(first_cut_committed_time - start_offset) + "\t" + "first cut committed from new shard")
print(str(shard_leave_request_time - start_offset) + "\t" + "shard requests to leave")
print(str(shard_finalized_time - start_offset) + "\t" + "shard finalized, last committed cut")
