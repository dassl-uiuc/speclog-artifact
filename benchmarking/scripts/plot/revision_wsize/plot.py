import numpy as np
import glob
import pandas as pd
import subprocess
import os 

results_dir = os.getenv("results_dir")

def get_append_metrics(path, df, ws):
    file_pattern = path + "append_metrics*.csv"

    total_throughput = 0
    latency_values = []

    for file in glob.glob(file_pattern):
        with open(file, 'r') as f:
            lines = f.readlines()[1:]
            for line in lines:
                parts = line.strip().split(',')
                gsn, latency, throughput = int(parts[0]), float(parts[1]), float(parts[2])
                latency_values.append(latency)
            
            total_throughput += throughput

    latency_array = np.array(latency_values)

    mean_latency = np.mean(latency_array)
    p50_latency = np.percentile(latency_array, 50)
    p99_latency = np.percentile(latency_array, 99)

    df.loc[ws] = {
        'mean_append_latency': mean_latency,
    }

    return df

def get_e2e_metrics_speclog(path, df, ws):
    file_pattern = path + "e2e_metrics*.csv"

    delivery_latency_values = []
    compute_latency_values = []
    confirmation_latency_values = []
    e2e_latency_values = []
    queuing_delay_values = []

    for file in glob.glob(file_pattern):
        with open(file, 'r') as f:
            lines = f.readlines()[1:]
            for line in lines:
                parts = line.strip().split(',')
                delivery, confirm, compute, e2e, queuing_delay = float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4]), float(parts[5])
                delivery_latency_values.append(delivery)
                compute_latency_values.append(compute)
                confirmation_latency_values.append(confirm)
                e2e_latency_values.append(e2e)
                queuing_delay_values.append(queuing_delay)

    
    delivery_latency_array = np.array(delivery_latency_values)
    compute_latency_array = np.array(compute_latency_values)
    confirmation_latency_array = np.array(confirmation_latency_values)
    e2e_latency_array = np.array(e2e_latency_values)
    queuing_delay_array = np.array([x for x in queuing_delay_values if x > 0])

    df.loc[ws] = {
        'mean_e2e_latency': np.mean(e2e_latency_array),
    }

    return df

results_dir = os.getenv("results_dir")
window_size = [5, 10, 20, 50, 100, 200, 300]
iters = 3

e2e_latencies = {}
append_latencies = {}
for iter in range(1, iters + 1):
    df_append_speclog = pd.DataFrame(columns=['mean_append_latency'])
    df_e2e_speclog = pd.DataFrame(columns=['mean_e2e_latency'])

    for ws in window_size:
        if ws not in e2e_latencies:
            e2e_latencies[ws] = []
        if ws not in append_latencies:
            append_latencies[ws] = []

        path = results_dir + f"/window_size_{iter}/" + str(ws) + "/" 
        for dir in glob.glob(path + "*/"):
            df_append_speclog = get_append_metrics(dir, df_append_speclog, ws)
            df_e2e_speclog = get_e2e_metrics_speclog(dir, df_e2e_speclog, ws)

        e2e_latencies[ws].append(df_e2e_speclog.loc[ws, 'mean_e2e_latency'])
        append_latencies[ws].append(df_append_speclog.loc[ws, 'mean_append_latency'])

print(e2e_latencies)
print(append_latencies)
# Write data to file
with open('ws_data', 'w') as f:
    f.write('ws\tmean_e2e_latency\tmean_append_latency\n')
    for ws in window_size:
        e2e_lat = np.mean(e2e_latencies[ws])
        append_lat = np.mean(append_latencies[ws])
        f.write(f'{ws}\t{e2e_lat:.2f}\t{append_lat:.2f}\n')


gnuplot_script = r"""
(cat <<EOF
	set terminal postscript eps enhanced color size 2.1, 1.5 font "Times-new-roman,19"
	set output "wsize.eps"
	set xlabel "Window Size" font "Times-new-roman,21" offset 0,0.5,0
	set ylabel "Latency (ms)" font "Times-new-roman,20" offset 1.5,-0.2,0
	set tmargin .5
	set lmargin 5
	set encoding utf8
	set rmargin 1.5
	set bmargin 3
	set yrange [0:]
	set xrange [5:300]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	#set ytics 0,200,800 scale 0.4
	#set xtics (1,3,5,7,10) scale 0.4
    set ytics 1 font "Times-new-roman,19"
    set xtics 50 font "Times-new-roman,19"
	set key bottom left font "Times-new-roman,20" samplen 2.5 maxrows 2 width -3
	set style function linespoints
	plot "ws_data" using (\$1):(\$2/1000) title "E2E Latency" with linespoints lc rgb 'red' dashtype 3 lw 5 pt 4 ps 1.3,\
		"ws_data" using  (\$1):(\$3/1000) title 'Append Latency' with linespoints lc rgb 'blue'  dashtype 1 lw 5 pt 3 ps 1.3,\
		
EOF
) | gnuplot -persist
"""

subprocess.run(['bash'], input=gnuplot_script, text=True)
subprocess.run(['bash'], input="epstopdf wsize.eps", text=True)
subprocess.run(['bash'], input="rm ws_data", text=True)