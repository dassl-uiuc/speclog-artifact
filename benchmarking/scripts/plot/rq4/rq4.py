#!/usr/bin/env python3
import re
import math
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from datetime import timedelta
import subprocess
from matplotlib.ticker import MultipleLocator, MaxNLocator, FuncFormatter

def find_nearest_multiple_of_3(number):
    return math.ceil(number / 3) * 3
    
def process_logs(logfile1, logfile2, logfile3):
    # Step 1: Extract the number from logfile1
    with open(logfile1, 'r') as file1:
        for line in file1:
            match = re.search(r"burst record with lsn (\d+)", line)
            if match:
                number = int(match.group(1))
                y = find_nearest_multiple_of_3(number)
                break
        else:
            raise ValueError("No 'burst record with lsn' found in logfile1")
    
    # Step 2: Locate and fetch lines from logfile2
    with open(logfile2, 'r') as file2:
        lines = file2.readlines()
    
    target_index = None
    for i, line in enumerate(lines):
        if f"cut:{y} cut:0" in line:
            target_index = i
            break
    
    if target_index is None:
        raise ValueError(f"No line containing 'cut:{y} cut:0' found in logfile2")
    
    # Fetch 2000 lines before and after
    start = max(0, target_index - 200)
    end = min(len(lines), target_index + 200)
    selected_lines = lines[start:end]
    
    # Step 3: Write to logfile3
    with open(logfile3, 'w') as file3:
        file3.writelines(selected_lines)

    return y


suffix = 'with_e2e'
logfile1 = "PATH/lagfix_" + suffix + "/data-0-0.log"
logfile2 = "PATH/lagfix_" + suffix + "/order-0.log"
logfile3 = "output.log"

y = process_logs(logfile1, logfile2, logfile3)

def extract_events(log_file, y):
    shard_0_cuts = []
    shard_1_cuts = []
    
    lagfix_detected = []
    lagfix_notified = []
    lagfix_resolved = []
    burst_start = []

    # Generic pattern for timestamps
    timestamp_pattern = r"(\d{2}:\d{2}:\d{2}\.\d{6})"

    with open(log_file, 'r') as file:
        for line in file:
            # Check for shard 0 cuts
            match_shard_0 = re.search(fr"{timestamp_pattern} cut:(\d+) cut:0", line)
            if match_shard_0:
                timestamp, cut = match_shard_0.groups()
                shard_0_cuts.append((timestamp, int(cut)))

            # Check for shard 1 cuts
            match_shard_1 = re.search(fr"{timestamp_pattern} localReplicaID:1 cut:0 cut:(\d+)", line)
            if match_shard_1:
                timestamp, cut = match_shard_1.groups()
                shard_1_cuts.append((timestamp, int(cut)))

            if burst_start == [] and "cut:" + str(y) + " cut:0" in line:
                burst_start.append(re.search(timestamp_pattern, line).group(1))

            if burst_start != [] and lagfix_detected == [] and "significant lag in cuts:" in line:
                lagfix_detected.append(re.search(timestamp_pattern, line).group(1))

            if burst_start != [] and lagfix_notified == [] and "adjustmentSignal:" in line:
                lagfix_notified.append(re.search(timestamp_pattern, line).group(1))
            
            if burst_start != [] and lagfix_resolved == [] and "fixedLag:true" in line:
                lagfix_resolved.append(re.search(timestamp_pattern, line).group(1))


    return shard_0_cuts, shard_1_cuts, lagfix_detected, lagfix_notified, lagfix_resolved, burst_start


# Example usage
log_file = "output.log"
shard_0_cuts, shard_1_cuts, lagfix_detected, lagfix_notified, lagfix_resolved, burst_start = extract_events(log_file, y)


## Execute helpers and get output
speclog_output_with = subprocess.run(['python3', 'helper_speclog.py', 'with'], capture_output=True, text=True)
speclog_output_without = subprocess.run(['python3', 'helper_speclog.py', 'without'], capture_output=True, text=True)
scalog_output = subprocess.run(['python3', 'helper_scalog.py'], capture_output=True, text=True)

bs_with, bs_without, bs_scalog = None, None, None
for line in speclog_output_with.stdout.splitlines():
    if "burst start time" in line:
        bs_with = float(line.split("\t")[0])
        break
for line in speclog_output_without.stdout.splitlines():
    if "burst start time" in line:
        bs_without = float(line.split("\t")[0])
        break
for line in scalog_output.stdout.splitlines():
    if "burst start time" in line:
        bs_scalog = float(line.split("\t")[0])
        break

true_bs = min(bs_with, bs_without)

def parse_timestamp(timestamp):
    return datetime.strptime(timestamp, "%H:%M:%S.%f")


timestamps = {}
def plot_events_zoomed(
    shard_0_cuts, shard_1_cuts, lagfix_detected, lagfix_notified, lagfix_resolved, burst_start, zoom_event="Lag Detected", zoom_window_ms=100
):
    # Convert timestamps to datetime objects
    shard_0_times = [parse_timestamp(t) for t, _ in shard_0_cuts]
    shard_0_values = [1 for _ in shard_0_cuts]  # Level 1 for Shard 0

    shard_1_times = [parse_timestamp(t) for t, _ in shard_1_cuts]
    shard_1_values = [2 for _ in shard_1_cuts]  # Level 2 for Shard 1

    # Parse event timestamps
    event_times = {
        "Lag Detected": [parse_timestamp(lagfix_detected[0])] if lagfix_detected else [],
        #"Lag Notified": [parse_timestamp(lagfix_notified[0])] if lagfix_notified else [],
        "Lag Resolved": [parse_timestamp(lagfix_resolved[0])] if lagfix_resolved else [],
        "Burst Start": [parse_timestamp(burst_start[0])] if burst_start else [],
    }

    # Event colors for differentiation
    event_colors = {
        "Lag Detected": "red",
        "Lag Notified": "purple",
        "Lag Resolved": "green",
        "Burst Start": "brown",
    }

    # Find the minimum timestamp across all events and cuts
    all_times = shard_0_times + shard_1_times
    for times in event_times.values():
        all_times.extend(times)

    min_timestamp = min(all_times)
    print (min_timestamp)
    # Convert all timestamps to relative time (milliseconds since min_timestamp)
    def to_relative_ms(time):
        return (time - min_timestamp).total_seconds() * 1000

    shard_0_times_rel = [to_relative_ms(t) for t in shard_0_times]
    shard_1_times_rel = [to_relative_ms(t) for t in shard_1_times]

    # Parse event times as relative times
    event_times_rel = {
        event_name: [to_relative_ms(t) for t in times]
        for event_name, times in event_times.items()
    }

    # Determine the time range for zooming
    if zoom_event not in event_times_rel or not event_times_rel[zoom_event]:
        print(f"No event found for {zoom_event}.")
        return

    zoom_time = event_times_rel[zoom_event][0]
    # print(zoom_time)
    zoom_start = zoom_time - zoom_window_ms
    zoom_end = zoom_time + zoom_window_ms

    # Filter data for the zoomed time range
    shard_0_times_zoomed = [t for t in shard_0_times_rel if zoom_start <= t <= zoom_end]
    shard_1_times_zoomed = [t for t in shard_1_times_rel if zoom_start <= t <= zoom_end]

    # shard_0_timestamps_zoomed = [t for t in shard_0_times if zoom_start <= to_relative_ms(t) <= zoom_end]
    # shard_1_timestamps_zoomed = [t for t in shard_1_times if zoom_start <= to_relative_ms(t) <= zoom_end]

    # Start plotting
    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot cut points for Shard 0 with stacking
    ax.scatter(shard_0_times_zoomed, [1 + i for i in range(len(shard_0_times_zoomed))], label="Shard 0 Cuts", color="blue", marker="o", s=40, alpha=0.6)

    # Plot cut points for Shard 1 with stacking
    ax.scatter(shard_1_times_zoomed, [1 + i for i in range(len(shard_1_times_zoomed))], label="Shard 1 Cuts", color="orange", marker="o", s=40, alpha=0.6)
    
    min_time_print = min(shard_0_times_zoomed[0], shard_1_times_zoomed[0])
    delta = true_bs - event_times_rel["Burst Start"][0]
    # print(delta) 
    with open('shard0_cuts', 'w') as f:
        for ii in range (0,len(shard_0_times_zoomed)):
            f.write(str(shard_0_times_zoomed[ii]+delta) + "\t" + str(ii) + "\n")

    with open('shard1_cuts', 'w') as f:
        for ii in range (0,len(shard_1_times_zoomed)):
            f.write(str(shard_1_times_zoomed[ii]+delta) + "\t" + str(ii) + "\n")
    
    # Plot vertical lines for events within the zoom window
    for event_name, times in event_times_rel.items():
        for time in times:
            if zoom_start <= time <= zoom_end:
                print (event_name,time+delta)
                timestamps[event_name] = time+delta
                ax.axvline(time, linestyle="--", label=event_name, color=event_colors[event_name], alpha=0.7)

    # Adjust x-axis for 1 ms ticks
    ax.xaxis.set_major_locator(MultipleLocator(1))  # 1 ms ticks
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:.0f}"))  # Show milliseconds as integers

    # Adjust number of ticks on the x-axis
    ax.xaxis.set_major_locator(MaxNLocator(nbins=10, prune='both'))  # Use nbins to control tick density

    # Set the x-limits to zoom in on the window
    ax.set_xlim(zoom_start, zoom_end)

    # Rotate x-axis labels for better readability
    ax.tick_params(axis='x', rotation=0)

    # Adjust y-axis
    #ax.set_yticks([1, 2])
    #ax.set_yticklabels(["Shard 0", "Shard 1"])

    # Add labels and legend
    ax.set_xlabel("Time (ms relative to min timestamp)")
    ax.set_ylabel("Reports")
    ax.set_title(f"Zoomed View: {zoom_event} (±{zoom_window_ms} ms)")
    ax.legend(loc="upper left", bbox_to_anchor=(1, 1), title="Legend", shadow=True)

    # Show the plot
    plt.tight_layout()
    plt.show()
    plt.savefig("output.png", dpi=600)


# Example usage (assuming shard_0_cuts, shard_1_cuts, lagfix_detected, lagfix_notified, lagfix_resolved, burst_start are defined)
plot_events_zoomed(shard_0_cuts, shard_1_cuts, lagfix_detected, lagfix_notified, lagfix_resolved, burst_start, zoom_event="Burst Start", zoom_window_ms=20)


gnuplot_script = rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 2.4, 1.7 font "Times-new-roman,19"
	set output "cuts.eps"
	set xlabel "Time (ms)" font "Times-new-roman,21" offset 0,0.7,0
	set ylabel "Report number" font "Times-new-roman,20" offset 1,-0.2,0
	set tmargin .2
	set lmargin 6
	set encoding utf8
	set rmargin 1.55
	set bmargin 2.5
	set yrange [0:33]
	set xrange [90:110]
	set xtics 10
	set tics scale 0.2
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	#set ytics 0,200,800 scale 0.4
	set key at 99.5,30 font "Times-new-roman,18" samplen 1.5 maxrows 5 width -5
	set style data points
	set pointsize 0.9
	set style line 1 lc rgb "blue" pt 7 # Blue points with circle symbol
	set style line 2 lc rgb "orange" pt 5 # Blue points with circle symbol
	set trange [-100:100]
	set parametric
	plot {timestamps["Burst Start"]},t title "Burst start" dt 3 lw 3 lc rgb 'black',\
		{timestamps["Lag Detected"]},t title "Detected" dt 1 lw 3,\
		{timestamps["Lag Resolved"]},t  title "Lag fixed" dt 2 lw 3,\
		"shard0_cuts" using 1:(\$2-10) title "Shard 1" with points linestyle 1,\
		"shard1_cuts" using 1:(\$2-10) title "Shard 2" with points linestyle 2



		
EOF
) | gnuplot -persist"""


gnuplot_script_lat = rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 2.3, 1.7 font "Times-new-roman,19"
	set output "lagfix_e2e.eps"
	set xlabel "Time (ms)" font "Times-new-roman,21" offset 0,0.4,0
	set ylabel "E2E Latency (ms)" font "Times-new-roman,20" offset 1,-0.2,0
	set tmargin 2.5
	set lmargin 6
	set encoding utf8
	set rmargin 0.5
	set bmargin 3
	set yrange [0:]
	set xrange [0:200]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	set tics scale 0.4
	set xtics 30
	#set ytics 0,200,800 scale 0.4
	#set xtics (1,3,5,7,10) scale 0.4
	set key at 200,13 font "Times-new-roman,20" samplen 2 maxrows 2 width -7 invert
	set style function linespoints
	set trange [0:10]
	set parametric
	plot {timestamps["Burst Start"]},t title "Burst start" dt 3 lw 3 ps 0 lc rgb 'black',\
		"withoute2e.csv" using 1:(\$2/1000) title 'Belfast no-lf' with lines lc rgb '#FFD689'  dashtype 1 lw 4,\
		"withe2e.csv" using 1:(\$2/1000) title 'Belfast' with lines lc rgb '#009988'  dashtype 1 lw 4,\
		"scalog-e2e.csv" using 1:(\$2/1000) title 'Scalog' with lines lc rgb 'coral'  dashtype 1 lw 5
		
EOF
) | gnuplot -persist

"""

subprocess.run(['bash'], input=gnuplot_script, text=True)
subprocess.run(['bash'], input=gnuplot_script_lat, text=True)
subprocess.run(['bash'], input="epstopdf cuts.eps", text=True)
subprocess.run(['bash'], input="epstopdf lagfix_e2e.eps", text=True)
subprocess.run(['bash'], input="rm *.eps output.log output.png *_cuts *.csv", text=True)
subprocess.run(['bash'], input="mv cuts.pdf 9b.pdf", text=True)
subprocess.run(['bash'], input="mv lagfix_e2e.pdf 9a.pdf", text=True)
