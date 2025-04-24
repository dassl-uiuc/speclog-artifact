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
logfile = "../../../results/scalog_qc/data-0-0.log"
burst_cut_num = extract_burst_cut(logfile)
print(f"Burst cut number: {burst_cut_num}")


# Helper function to parse timestamps
def parse_timestamp(ts):
    return datetime.strptime(ts, "%H:%M:%S.%f")

# File path
log_file = "../../../results/scalog_qc/order-0.log"

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

assert len(timestamps) == len(tput_values)
zs = to_relative_ms(zoom_start)
ze = to_relative_ms(zoom_end)

with open("scalog-tput", 'w') as f:
    for j in range(0, len(timestamps)):
        curr_ts = to_relative_ms(timestamps[j])
        if curr_ts >= zs and curr_ts <= ze: 
            f.write(str(curr_ts - zs) + "\t" + str(tput_values[j]) + "\n")

burst_start_rel = to_relative_ms(burst_start_time) -zs
print(str(burst_start_rel) + "\t" + "Long-term client started")

'''
# Plot
fig, ax = plt.subplots(figsize=(10, 6))

# Plot throughput
ax.plot([to_relative_ms(t) for t in timestamps], tput_values, label="Throughput (ops/sec)", color="blue")

# Plot total throughput
ax.plot([to_relative_ms(t) for t in total_tput_timestamps], total_tput_values, label="Total Throughput (ops/sec)", color="green")

# # Plot Shard 0 cuts
# ax.scatter(shard_0_times_rel, [1 + 0.05 * i for i in range(len(shard_0_times_rel))],
#            label="Shard 0 Cuts", color="blue", marker="o", s=40, alpha=0.6)

# # Plot Shard 1 cuts
# ax.scatter(shard_1_times_rel, [2 + 0.05 * i for i in range(len(shard_1_times_rel))],
#            label="Shard 1 Cuts", color="orange", marker="o", s=40, alpha=0.6)

# Plot quota changes
# for t_rel, (quota_0, quota_1) in quota_times_rel.items():
#     ax.axvline(t_rel, color="green", linestyle="--", alpha=0.8)
#     ax.text(t_rel, 3, f"Q0:{quota_0}\nQ1:{quota_1}", color="green", fontsize=8, rotation=45)

# Highlight burst start


# Labels and legend
ax.set_xlabel("Relative Time (ms)")
ax.set_ylabel("Event Level")
ax.set_xlim(to_relative_ms(zoom_start), to_relative_ms(zoom_end))
ax.set_title("Event Timeline around Burst Start")
ax.legend()
ax.grid(True, linestyle="--", alpha=0.5)

plt.tight_layout()
plt.show()
'''
