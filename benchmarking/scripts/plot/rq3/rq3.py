import os
import csv
import numpy as np 
import pandas as pd
import subprocess

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


root_path="../../../results/"

df_scalog = pd.DataFrame(columns=["mean", "stddev"])
df_speclog = pd.DataFrame(columns=["mean", "stddev"])
tputs = [20, 40, 100]

for tput in tputs:
    scalog_path = root_path + "append_scalog/"
    means = np.array([])
    for i in [1, 2, 3]:
        scalog_path = root_path + "append_scalog/" + str(i) + "/1ms/append_bench_" + str(tput)
        latencies = get_latencies(scalog_path)
        mean, p50, p99 = get_latency_metrics(latencies)
        means = np.append(means, mean)
    df_scalog.loc[tput] = [means.mean(), means.std(ddof=1)]

    
    means = np.array([])
    for i in [1, 2, 3]:
        speclog_path = root_path + "append_wo/" + str(i) + "/1ms/append_bench_" + str(tput)
        if tput == 100:
            speclog_path = root_path + "append_wi/" + str(i) + "/1ms/append_bench_" + str(tput)
        latencies = get_latencies(speclog_path)
        mean, p50, p99 = get_latency_metrics(latencies)
        means = np.append(means, mean)
    df_speclog.loc[tput] = [means.mean(), means.std(ddof=1)]

output_df = pd.DataFrame({
    '#shards': [2, 4, 10],
    'speclog': df_speclog['mean'].values*1000,
    'scalog': df_scalog['mean'].values*1000,
    'stdev-spec': df_speclog['stddev'].values,
    'stdev-scalog': df_scalog['stddev'].values
})

output_df.to_csv('data', sep='\t', index=False, float_format='%.10f')

gnuplot_script=r"""
#! /bin/bash

(cat <<EOF
set terminal postscript eps enhanced color size 2.4, 2 font "Times-new-roman,23"
set output 'rq3-append-lat.eps'

#set yrange [0:20]
set style line 2 lc rgb 'black' lt 1 lw 2
set style data histogram 
set style histogram cluster gap 2 errorbars lw 2
set style fill pattern border 0
set xtics format "" scale 0
set boxwidth 0.975
set ytics scale 0.5
set ytics 2
#set format y "10^{%T}"
set xrange [-0.5:2.5]
set yrange [0:4.5]
set tmargin 1.6
set lmargin 5
set rmargin 1
set bmargin 3.75

set label '\\@20K' at 0,-1.2 center 
set label '\\@40K' at 1,-1.2 center 
set label '\\@100K' at 2,-1.2 center 

set key at 2.6,5.3 font "Times-new-roman,23" samplen 2.5 maxrows 1
set xlabel "Shards" font "Times-new-roman,23" offset 0,-.7,0
set ylabel "Append Latency (ms)" font "Times-new-roman,23" offset 0.5,0,0
plot 'data' using (\$3/1000):5:xtic(1) title "Scalog" fs pattern 7 border rgb "coral" lc rgb 'coral' lw 2,\
 	 'data' using (\$2/1000):4 title "Belfast" fs pattern 3 border rgb "#009988" lc rgb '#009988' lw 2

EOF
) | gnuplot -persist
"""

subprocess.run(['bash'], input=gnuplot_script, text=True)
subprocess.run(['bash'], input="epstopdf rq3-append-lat.eps", text=True)
subprocess.run(['bash'], input="rm *.eps data", text=True)
subprocess.run(['bash'], input="mv rq3-append-lat.pdf 8.pdf", text=True)


