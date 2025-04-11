import re
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from datetime import timedelta
from matplotlib.ticker import MultipleLocator, MaxNLocator, FuncFormatter
import pandas as pd
import subprocess


straggler_pattern = r"start slow report local cut number (\d+), window num (\d+)"

def get_straggler_lsn_wsn(path):
    with open(path+"data-1-1.log") as f:
        lines = f.readlines()
        for line in lines:
            match = re.search(straggler_pattern, line)
            if match:
                return match.group(1), match.group(2)
            

path = "PATH/slowshard/"
lsn, wn = get_straggler_lsn_wsn(path)
print(f"lsn: {lsn}, wn: {wn}")


def parse_timestamp(timestamp):
    return datetime.strptime(timestamp, "%H:%M:%S.%f")

def extract_slowshard_events(log_file, lsn, wn):
    first_slow_report = []
    slow_shard_detected = []
    slow_shard_notified = []
    slow_shard_quarantine_end = []

    timestamps = []
    tput_values = []
    total_tput_timestamps = []
    total_tput_values = []

    # Generic pattern for timestamps
    timestamp_pattern = r"(\d{2}:\d{2}:\d{2}\.\d{6})"
    tput_pattern = r"\[real-time tput\]: (\d+) ops/sec"
    total_tput_pattern = r"\[real-time total tput\]: (\d+) ops/sec"
    slow_report_pattern = fr"shardID:1 localReplicaID:1.*?localCutNum:{lsn} windowNum:{wn}"

    with open(log_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            tput_match = re.search(tput_pattern, line)
            total_tput_match = re.search(total_tput_pattern, line)
            timestamp_match = re.search(timestamp_pattern, line)
            
            if tput_match and timestamp_match:
                tput_values.append(int(tput_match.group(1)))
                timestamps.append(parse_timestamp(timestamp_match.group(1)))

            if total_tput_match and timestamp_match:
                total_tput_values.append(int(total_tput_match.group(1)))
                total_tput_timestamps.append(parse_timestamp(timestamp_match.group(1)))

            if first_slow_report == [] and re.search(slow_report_pattern, line):
                first_slow_report.append(parse_timestamp(timestamp_match.group(1)))

            if first_slow_report != [] and slow_shard_detected == [] and "slow shard: 1, being grounded for 1 window" in line:
                slow_shard_detected.append(parse_timestamp(timestamp_match.group(1)))

            if first_slow_report != [] and slow_shard_notified == [] and "replica 3 grounded for 4 windows" in line:
                slow_shard_notified.append(parse_timestamp(timestamp_match.group(1)))

            if first_slow_report != [] and slow_shard_quarantine_end == [] and "ungrounding replica 3" in line:
                slow_shard_quarantine_end.append(parse_timestamp(timestamp_match.group(1)))
            
    return first_slow_report, slow_shard_detected, slow_shard_notified, slow_shard_quarantine_end, timestamps, tput_values, total_tput_timestamps, total_tput_values


# Example usage
log_file = path + "order-0.log"
first_slow_report, slow_shard_detected, slow_shard_notified, slow_shard_quarantine_end, timestamps, tput_values, total_tput_timestamps, total_tput_values = extract_slowshard_events(log_file, lsn, wn)

event_start = first_slow_report[0]


# CONFIGURE ZOOM WINDOW HERE
zoom_window_ms = 2000
zoom_start = event_start - timedelta(milliseconds=zoom_window_ms)
zoom_end = event_start + timedelta(milliseconds=zoom_window_ms)

# Convert to relative times
all_ts = timestamps

min_timestamp = min(all_ts)
print(min_timestamp)

def to_relative_ms(t):
    return (t - min_timestamp).total_seconds() * 1000

# Plot
fig, ax = plt.subplots(figsize=(10, 6))

# Plot throughput
ax.plot([to_relative_ms(t) for t in timestamps], tput_values, label="Throughput (ops/sec)", color="blue")

# Plot total throughput
# ax.plot([to_relative_ms(t) for t in total_tput_timestamps], total_tput_values, label="Total Throughput (ops/sec)", color="green")

# Plot vertical lines for first_slow_report and related events with different colors
event_colors = {
    "First Slow Report": "red",
    "Slow Shard Detected": "purple",
    "Slow Shard Notified": "green",
    "Slow Shard Quarantine End": "brown",
}

for event_name, times in {
    "First Slow Report": first_slow_report,
    "Slow Shard Detected": slow_shard_detected,
    "Slow Shard Notified": slow_shard_notified,
    "Slow Shard Quarantine End": slow_shard_quarantine_end,
}.items():
    for time in times:
        ax.axvline(to_relative_ms(time), linestyle="--", label=event_name, color=event_colors[event_name], alpha=0.7)


# Labels and legend
ax.set_xlabel("Relative Time (ms)")
ax.set_ylabel("Event Level")
ax.set_xlim(to_relative_ms(zoom_start), to_relative_ms(zoom_end))
ax.set_title("Event Timeline around Slow Shard")
ax.set_ylim(0, 14000)
ax.legend()
ax.grid(True, linestyle="--", alpha=0.5)

plt.tight_layout()
#plt.savefig("tput.png", dpi=600)

def parse_timestamp(timestamp):
    return datetime.strptime(timestamp, "%H:%M:%S.%f")

def get_latencies(path):
    e2e_file_pattern = [path + "e2e_metrics_0.csv", path + "e2e_metrics_1.csv"]
    e2e_latency_values = {}
    straggler_trigger = None

    for file in e2e_file_pattern:
        with open(file, 'r') as f:
            lines = f.readlines()[1:]
            for line in lines:
                parts = line.strip().split(',')
                gsn, e2e_latency, delivery_timestamp = int(parts[0]), int(parts[4]), parts[6]
                e2e_latency_values[gsn] = [e2e_latency, parse_timestamp(delivery_timestamp)]
    
    client_file = path + "client_node14_3.log"
    with open(client_file, 'r') as f:
        log_data = f.read()
        for line in log_data.splitlines():
            if "triggered straggler" in line:
                timestamp_match = re.search(r"(\d{2}:\d{2}:\d{2}\.\d{6})", line)
                if timestamp_match:
                    straggler_trigger = parse_timestamp(timestamp_match.group(1))
                    break

    return e2e_latency_values, straggler_trigger

# CONFIGURE PATH HERE
path = "PATH/slowshard/"
e2e_latency_values, straggler_trigger = get_latencies(path)
## e2e latency plots

latency_times = []
latencies = []
for gsn, (latency, timestamp) in e2e_latency_values.items():
    latency_times.append(timestamp)
    latencies.append(latency)

df = pd.DataFrame({
    'time': latency_times,
    'latency': latencies
})

df = df.sort_values(by='time')

# CONFIGURE WINDOW SIZE HERE
window_size = 10  # Set the window size for the moving average
df['moving_avg'] = df['latency'].rolling(window=window_size, min_periods=1).mean()

min_time = df['time'].min()
df['relative_time_ms'] = (df['time'] - min_time).dt.total_seconds() * 1000

# CONFIGURE ZOOM IN PERIOD HERE
start_time = straggler_trigger - timedelta(milliseconds=1000)  
end_time = straggler_trigger + timedelta(milliseconds=1000)  

df_zoomed = df[(df['time'] >= start_time) & (df['time'] <= end_time)]

plt.figure(figsize=(10, 6))
plt.plot(df_zoomed['relative_time_ms'], df_zoomed['moving_avg'], label=f'Moving Average (window={window_size})', color='blue', linewidth=2)

with open("e2edata", 'w') as f:
    for key in df_zoomed['relative_time_ms'].keys():
        f.write(str(df_zoomed['relative_time_ms'][key]) + "\t" + str(df_zoomed['moving_avg'][key]) + "\n")

start_time_relative = (straggler_trigger - min_time).total_seconds() * 1000
straggler_quarantined = (slow_shard_notified[0] - min_time).total_seconds() * 1000
straggler_quarantine_ended = (slow_shard_quarantine_end[0] - min_time).total_seconds() * 1000
plt.axvline(x=start_time_relative, color='green', linestyle=':', label=f'straggler triggered')
plt.axvline(x=straggler_quarantined, color='red', linestyle=':', label=f'straggler quarantined')
plt.axvline(x=straggler_quarantine_ended, color='purple', linestyle=':', label=f'straggler quarantine ended')

print("min_time:" + str(min_time))
print("start_time_relative:" + str(start_time_relative))
print("straggler_quarantined:" + str(straggler_quarantined))
print("straggler_quarantine_ended:" + str(straggler_quarantine_ended))


plt.xlabel('Time (ms)')
plt.ylabel('Latency (us)')
plt.title('e2e Latency Over Time with Moving Average')
plt.xticks(rotation=45)
# plt.ylim(ymin=0, ymax=16000)
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("output.png", dpi=600)

min_time = min(df_zoomed['relative_time_ms'])
gnuplot_script = rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 2.7, 1.5 font "Times-new-roman,19"
	set output "straggler_e2e.eps"
	set xlabel "Time (ms)" font "Times-new-roman,21" offset 0,0.4,0
	set ylabel "E2E Latency (ms)" font "Times-new-roman,20" offset 1,-0.2,0
	set tmargin 0.5
	set lmargin 5.5
	set encoding utf8
	set rmargin 0.5
	set bmargin 3
	set yrange [0:]
	set xrange [0:2000]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	set xtics 0,500,1800 scale 0.4
	#set ytics 0,200,800 scale 0.4
	set tics scale 0.4
	set key at 999,24.5 font "Times-new-roman,20" samplen 1.5 maxrows 4 width -3
	set style function linespoints
	set trange [0:30]
	set parametric
	plot 19923.52-{min_time},t title "straggle starts" dt 1 lw 4 ps 0 lc rgb 'purple',\
		 20061.77-{min_time},t title "quota set to 0" dt 2 lw 4 ps 0 lc rgb 'blue',\
		 20477.443-{min_time},t title "quota reset" dt 3 lw 4 ps 0 lc rgb 'black',\
		 "e2edata" using (\$1-{min_time}):(\$2/1000) title 'Belfast' with lines lc rgb '#009988'  dashtype 1 lw 4
		
EOF
) | gnuplot -persist
"""

subprocess.run(['bash'], input=gnuplot_script, text=True)
subprocess.run(['bash'], input="epstopdf straggler_e2e.eps", text=True)
subprocess.run(['bash'], input="rm *.eps *.png e2edata", text=True)
subprocess.run(['bash'], input="mv straggler_e2e.pdf 13.pdf", text=True)