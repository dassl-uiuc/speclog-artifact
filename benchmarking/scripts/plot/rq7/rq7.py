import numpy as np
import glob
import pandas as pd
import subprocess
import os
import matplotlib.pyplot as plt
import os 

results_dir = os.getenv("results_dir")
num_iter = 1

def get_append_metrics(path, df):
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

    num_shards = int(path.split("_")[-1].split("/")[0]) 

    df.loc[num_shards] = {
        'throughput': total_throughput,
        'mean_e2e_latency': 0.0,
    }

    return df

def get_e2e_metrics_speclog(path, df):
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

    num_shards = int(path.split("_")[-1].split("/")[0])

    df.loc[num_shards, 'mean_e2e_latency'] = np.mean(e2e_latency_array)

    return df

def get_e2e_metrics_scalog(path, df):
    file_pattern = path + "e2e_metrics*.csv"

    delivery_latency_values = []
    e2e_latency_values = []
    queuing_delay_values = []

    for file in glob.glob(file_pattern):
        with open(file, 'r') as f:
            lines = f.readlines()[1:]
            for line in lines:
                parts = line.strip().split(',')
                delivery, e2e, queuing_delay = float(parts[1]), float(parts[2]), float(parts[3])
                delivery_latency_values.append(delivery)
                e2e_latency_values.append(e2e)
                queuing_delay_values.append(queuing_delay)

    
    delivery_latency_array = np.array(delivery_latency_values)
    e2e_latency_array = np.array(e2e_latency_values)
    queuing_delay_array = np.array([x for x in queuing_delay_values if x > 0])

    num_shards = int(path.split("_")[-1].split("/")[0])
    df.loc[num_shards, 'mean_e2e_latency'] = np.mean(e2e_latency_array)

    return df


df_speclog = pd.DataFrame(columns=[
    'throughput', 'mean_e2e_latency'
])

df_scalog = pd.DataFrame(columns=[
    'throughput', 'mean_e2e_latency'
])


dfs = {}
for run in range(1, num_iter + 1):
    scalog = pd.DataFrame(columns=[
        'throughput', 'mean_e2e_latency'
    ])
    speclog = pd.DataFrame(columns=[
        'throughput', 'mean_e2e_latency'
    ])

    for shards in [1, 2, 3, 4, 5]:
        path_scalog = results_dir + f"/e2e_scalability/runs_3_scalog/{run}/e2e_1200_{shards}/"
        path_speclog = results_dir + f"/e2e_scalability/runs_3_wo_sc/{run}/e2e_1200_{shards}/"
        if shards >= 3: 
            path_speclog = results_dir + f"/e2e_scalability/runs_3_wi_sc/{run}/e2e_1200_{shards}/" 
        scalog = get_append_metrics(path_scalog, scalog)
        scalog = get_e2e_metrics_scalog(path_scalog, scalog)
        speclog = get_append_metrics(path_speclog, speclog)
        speclog = get_e2e_metrics_speclog(path_speclog, speclog)

    dfs[run] = {
        'scalog': scalog,
        'speclog': speclog
    }

# avg over runs
for run in range(1, num_iter + 1):
    scalog = dfs[run]['scalog']
    speclog = dfs[run]['speclog']

    if run == 1:
        df_speclog = speclog
        df_scalog = scalog
    else:
        df_speclog += speclog
        df_scalog += scalog

df_speclog = df_speclog / num_iter
df_scalog = df_scalog / num_iter


with open("data", "w") as f:
    f.write("#shards\tSpeclog\tScalog\n")
    for shards in df_speclog.index:
        f.write(f"{shards*2}\t{df_speclog.loc[shards]['throughput']}\t{df_scalog.loc[shards]['throughput']}\n")

with open("lat_data", "w") as f:
    f.write("#shards\tSpeclog\tScalog\n")
    for shards in df_speclog.index:
        f.write(f"{shards*2}\t{df_speclog.loc[shards]['mean_e2e_latency']}\t{df_scalog.loc[shards]['mean_e2e_latency']}\n")

latencies_wo_staggering = {"e2e": []}
latencies_wi_staggering = {"e2e": []}
latencies_scalog = {"e2e": []}


wo_staggering = results_dir + f"/e2e_scalability/runs_3_wo_sc/1/e2e_1200_5/"
wi_staggering = results_dir + f"/e2e_scalability/runs_3_wi_sc/1/e2e_1200_5/"
scalog = results_dir + f"/e2e_scalability/runs_3_scalog/1/e2e_1200_5/"


for file_name in os.listdir(wo_staggering):
    if file_name.startswith("e2e") and file_name.endswith(".csv"):
        file_path = os.path.join(wo_staggering, file_name)
        df = pd.read_csv(file_path)
        latencies_wo_staggering['e2e'].extend(df['e2e latency (us)'])

for file_name in os.listdir(wi_staggering):
    if file_name.startswith("e2e") and file_name.endswith(".csv"):
        file_path = os.path.join(wi_staggering, file_name)
        df = pd.read_csv(file_path)
        latencies_wi_staggering['e2e'].extend(df['e2e latency (us)'])


for file_name in os.listdir(scalog):
    if file_name.startswith("e2e") and file_name.endswith(".csv"):
        file_path = os.path.join(scalog, file_name)
        df = pd.read_csv(file_path)
        latencies_scalog['e2e'].extend(df['e2e latency (us)'])


for key, value in latencies_wi_staggering.items():
    latencies_wi_staggering[key] = np.sort(np.array(value))

for key, value in latencies_wo_staggering.items():
    latencies_wo_staggering[key] = np.sort(np.array(value))

for key, value in latencies_scalog.items():
    latencies_scalog[key] = np.sort(np.array(value))


cdf_wi_staggering = {'e2e': []}
cdf_wo_staggering = {'e2e': []}
cdf_scalog = {'e2e': []}

# get array of percentiles from 1 to 99
percentiles = np.linspace(0.01, 100.0, 10000)
for key, value in latencies_wi_staggering.items():
    cdf_wi_staggering[key] = np.percentile(value, percentiles)
for key, value in latencies_wo_staggering.items():
    cdf_wo_staggering[key] = np.percentile(value, percentiles)
for key, value in latencies_scalog.items():
    cdf_scalog[key] = np.percentile(value, percentiles)


with open("cdfdata", 'w') as f:
    f.write("#percentile\two\twi\tscalog\n")
    for i in range(len(cdf_wi_staggering['e2e'])):
        f.write(f"{percentiles[i]:.2f}\t{cdf_wo_staggering['e2e'][i]:.2f}\t{cdf_wi_staggering['e2e'][i]:.2f}\t{cdf_scalog['e2e'][i]:.2f}\n")


gnuplot_lat = rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 2.3, 1.8 font "Times-new-roman,19"
	set output "scalee2e.eps"
	set xlabel "Shards" font "Times-new-roman,22" offset 0,0
	set ylabel "E2E Latency (ms)" font "Times-new-roman,22" offset .5,-0.2,0
	set tmargin .5
	set lmargin 6
	set encoding utf8
	set rmargin 1.2
	set bmargin 3
	set yrange [0:6]
	set xrange [0:11]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	#set ytics 0,25,110 scale 0.4
	#set xtics (1,3,5,7,10) scale 0.4
	set key at 10,2.5 font "Times-new-roman,22" samplen 2.5 maxrows 2
	set style function linespoints
	plot "lat_data" using 1:(\$3/1000) title "Scalog" with linespoints lc rgb 'coral' dashtype 3 lw 5 pt 4 ps 1.3,\
		"lat_data" using 1:(\$2/1000) title 'Belfast' with linespoints lc rgb '#009988'  dashtype 1 lw 5 pt 3 ps 1.3,\
		
EOF
) | gnuplot -persist
"""

gnuplot_tput = rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 2.3, 1.7 font "Times-new-roman,19"
	set output "scaletput.eps"
	set xlabel "Shards" font "Times-new-roman,22" offset 0,0,0
	set ylabel "Throughput (KOps/s)" font "Times-new-roman,22" offset 1,-0.2,0
	set tmargin .5
	set lmargin 6.5
	set encoding utf8
	set rmargin 1.2
	set bmargin 3
	set yrange [0:110]
	set xrange [0:]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	set ytics 0,25,110 scale 0.4
	#set xtics (1,3,5,7,10) scale 0.4
	set key at 10,35 font "Times-new-roman,22" samplen 2.5 maxrows 2
	set style function linespoints
	plot "data" using 1:(\$3/1000) title "Scalog" with linespoints lc rgb 'coral' dashtype 3 lw 5 pt 4 ps 1.3,\
		"data" using 1:(\$2/1000) title 'Belfast' with linespoints lc rgb '#009988'  dashtype 1 lw 5 pt 3 ps 1.3,\
		
EOF
) | gnuplot -persist
"""

gnuplot_cdf = rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 2.3, 1.7 font "Times-new-roman,19"
	set output "scalecdf.eps"
	set xlabel "E2E Latency (ms)" font "Times-new-roman,22" offset 0,0.2,0
	set ylabel "CDF" font "Times-new-roman,22" offset 1.5,0,0
	set tmargin 3.1
	set lmargin 6
	set encoding utf8
	set rmargin 1.2
	set bmargin 3
	#set yrange [0:7.2]
	set xrange [0:10]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	set ytics 20
	#set ytics 0,25,110 scale 0.4
	#set xtics (1,3,5,7,10) scale 0.4
	set key at 10.5,143 font "Times-new-roman,21" samplen 1.4 maxrows 2 width -13
	set style function linespoints
	plot "cdfdata" using (\$4/1000):1 title "Scalog" with lines lc rgb 'coral' dashtype 3 lw 5,\
		"cdfdata" using (\$2/1000):1 title "Belfast no-stagger" with lines lc rgb '#A9A9A9' dashtype 2 lw 4,\
		"" using (\$3/1000):1 title 'Belfast' with lines lc rgb '#009988'  dashtype 1 lw 4,\

		
EOF
) | gnuplot -persist
"""

subprocess.run(['bash'], input=gnuplot_lat, text=True)
subprocess.run(['bash'], input=gnuplot_tput, text=True)
subprocess.run(['bash'], input=gnuplot_cdf, text=True)
subprocess.run(['bash'], input="epstopdf scalee2e.eps", text=True)
subprocess.run(['bash'], input="epstopdf scaletput.eps", text=True)
subprocess.run(['bash'], input="epstopdf scalecdf.eps", text=True)
subprocess.run(['bash'], input="rm scalee2e.eps scaletput.eps scalecdf.eps data lat_data cdfdata", text=True)
subprocess.run(['bash'], input="mv scaletput.pdf 12a.pdf", text=True)
subprocess.run(['bash'], input="mv scalee2e.pdf 12b.pdf", text=True)
subprocess.run(['bash'], input="mv scalecdf.pdf 12c.pdf", text=True)
