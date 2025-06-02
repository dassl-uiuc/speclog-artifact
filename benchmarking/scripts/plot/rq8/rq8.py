import numpy as np
import glob
import pandas as pd
import subprocess
import os
import matplotlib.pyplot as plt
import os 

results_dir = os.getenv("results_dir")
num_iter = 3

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


for run in range(1, num_iter + 1):
    latencies_wo_staggering = {"e2e": []}
    latencies_wi_staggering = {"e2e": []}
    latencies_scalog = {"e2e": []}

    wo_staggering = results_dir + f"/e2e_scalability/runs_3_wo_sc/{run}/e2e_1200_5/"
    wi_staggering = results_dir + f"/e2e_scalability/runs_3_wi_sc/{run}/e2e_1200_5/"
    scalog = results_dir + f"/e2e_scalability/runs_3_scalog/{run}/e2e_1200_5/"


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


    with open(f"cdfdata_{run}", 'w') as f:
        f.write("#percentile\two\twi\tscalog\n")
        for i in range(len(cdf_wi_staggering['e2e'])):
            f.write(f"{percentiles[i]:.2f}\t{cdf_wo_staggering['e2e'][i]:.2f}\t{cdf_wi_staggering['e2e'][i]:.2f}\t{cdf_scalog['e2e'][i]:.2f}\n")


queueing_delay = {'1200': 600, '2000': 1000}
delivery_add = 1000
append_add = 700

import os
import re
import fnmatch

def average_emulation_metrics_speclog(directory):
    """
    Calculates the average of the `mean`, `p50`, and `p99` metrics across all files in a directory matching the pattern `data*.log`.

    Args:
        directory (str): Path to the directory containing log files.

    Returns:
        dict: A dictionary containing the averages of `mean`, `p50`, and `p99` metrics.
    """
    data_regex = re.compile(r"(mean|p50|p99),\s*([\d.e+-]+),\s*([\d.e+-]+)")
    tput_regex = re.compile(r"tput: ([\d.]+)")
    
    append_metrics = {"mean": [], "p50": [], "p99": []}
    delivery_metrics = {"mean": [], "p50": [], "p99": []}
    tput_metrics = {"mean": []}

    for filename in os.listdir(directory):
        if fnmatch.fnmatch(filename, 'data*.log'):  
            filepath = os.path.join(directory, filename)
            
            # Temporary dictionaries to track the last occurrence in the current file
            last_append_metrics = {}
            last_delivery_metrics = {}
            last_tput = {}

            with open(filepath, 'r') as file:
                for line in file:
                    match = data_regex.search(line)
                    if match:
                        metric, append_latency, delivery_latency = match.groups()
                        # Update the last seen values for the current file
                        last_append_metrics[metric] = float(append_latency)
                        last_delivery_metrics[metric] = float(delivery_latency)
                    if "tput:" in line:
                        tput = float(line.split()[-1])
                        last_tput["mean"] = tput

            # Append the last occurrence values from the current file to the global lists
            for metric in append_metrics.keys():
                if metric in last_append_metrics:
                    append_metrics[metric].append(last_append_metrics[metric])
                if metric in last_delivery_metrics:
                    delivery_metrics[metric].append(last_delivery_metrics[metric])
            if "mean" in last_tput:
                tput_metrics["mean"].append(last_tput["mean"])

    # Compute averages across all files for the last occurrences
    averages = {
        "append": {
            metric: sum(values) / len(values) if values else None
            for metric, values in append_metrics.items()
        },
        "delivery": {
            metric: sum(values) / len(values) if values else None
            for metric, values in delivery_metrics.items()
        },
        "tput": {
            metric: sum(values) if values else None
            for metric, values in tput_metrics.items()
        }
    }
    print("found values across " + str(len(append_metrics["mean"])) + " files")
    
    return averages

def average_emulation_metrics_scalog(directory):
    """
    Calculates the average of the `mean`, `p50`, and `p99` metrics across all files in a directory matching the pattern `data*.log`.

    Args:
        directory (str): Path to the directory containing log files.

    Returns:
        dict: A dictionary containing the averages of `mean`, `p50`, and `p99` metrics.
    """
    data_regex = re.compile(r"(mean|p50|p99),\s*([\d.e+-]+),\s*([\d.e+-]+)")
    
    append_metrics = {"mean": [], "p50": [], "p99": []}
    delivery_metrics = {"mean": [], "p50": [], "p99": []}
    tput_metrics = {"mean": []}

    for filename in os.listdir(directory):
        if fnmatch.fnmatch(filename, 'data*.log'):  
            filepath = os.path.join(directory, filename)
            
            # Temporary dictionaries to track the last occurrence in the current file
            last_append_metrics = {}
            last_delivery_metrics = {}
            last_tput = {}

            with open(filepath, 'r') as file:
                for line in file:
                    match = data_regex.search(line)
                    if match:
                        metric, append_latency, delivery_latency = match.groups()
                        # Update the last seen values for the current file
                        last_append_metrics[metric] = float(append_latency)
                        last_delivery_metrics[metric] = float(delivery_latency)
                    if "tput:" in line:
                        tput = float(line.split()[-1])
                        last_tput["mean"] = tput

            # Append the last occurrence values from the current file to the global lists
            for metric in append_metrics.keys():
                if metric in last_append_metrics:
                    append_metrics[metric].append(last_append_metrics[metric])
                if metric in last_delivery_metrics:
                    delivery_metrics[metric].append(last_delivery_metrics[metric])
            if "mean" in last_tput:
                tput_metrics["mean"].append(last_tput["mean"])


    # Compute averages across all files for the last occurrences
    averages = {
        "append": {
            metric: sum(values) / len(values) if values else None
            for metric, values in append_metrics.items()
        },
        "delivery": {
            metric: sum(values) / len(values) if values else None
            for metric, values in delivery_metrics.items()
        },
        "tput": {
            metric: sum(values) if values else None
            for metric, values in tput_metrics.items()
        }
    }
    print("found values across " + str(len(append_metrics["mean"])) + " files")
    
    return averages


with open("lat_tput_data_em", "w") as f:
    f.write("#shards\tspeclog1.2\tspeclog2\tscalog1.2\tscalog2\tSpecLog\tScalog\n")
    for num_shards in [6, 8, 10, 12, 16, 20]:
        input_directory = results_dir + "/speclog_em/emulation_" + str(num_shards)
        averages_speclog = average_emulation_metrics_speclog(input_directory)

        input_directory = results_dir + "/scalog_em/emulation_" + str(num_shards)
        averages_scalog = average_emulation_metrics_scalog(input_directory)

        speclog_e2e_12 = max(averages_speclog['append']['mean'] + append_add, averages_speclog['delivery']['mean'] + delivery_add + 1200 + queueing_delay['1200'])
        scalog_e2e_12 = averages_scalog['delivery']['mean'] + delivery_add + 1200 + queueing_delay['1200']

        speclog_e2e_2 = max(averages_speclog['append']['mean'] + append_add, averages_speclog['delivery']['mean'] + delivery_add + 2000 + queueing_delay['2000'])
        scalog_e2e_2 = averages_scalog['delivery']['mean'] + delivery_add + 2000 + queueing_delay['2000']

        f.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(2*num_shards, round(speclog_e2e_12, 2), round(speclog_e2e_2, 2), round(scalog_e2e_12, 2), round(scalog_e2e_2, 2), round(averages_speclog['tput']['mean'], 2), round(averages_scalog['tput']['mean'], 2)))


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

gnuplot_cdf = lambda run: rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 2.3, 1.7 font "Times-new-roman,19"
	set output "scalecdf_{run}.eps"
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
	plot "cdfdata_{run}" using (\$4/1000):1 title "Scalog" with lines lc rgb 'coral' dashtype 3 lw 5,\
		"cdfdata_{run}" using (\$2/1000):1 title "Belfast no-stagger" with lines lc rgb '#A9A9A9' dashtype 2 lw 4,\
		"" using (\$3/1000):1 title 'Belfast' with lines lc rgb '#009988'  dashtype 1 lw 4,\

		
EOF
) | gnuplot -persist
"""


gnuplot_lat_tput = rf"""
(cat <<EOF
	set terminal postscript eps enhanced color size 3, 2.1 font "Times-new-roman,19"
	set output "lat_tput_em.eps"
	set xlabel "Throughput (KOps/s)" font "Times-new-roman,21" offset 0,0.5,0
	set ylabel "E2E Latency (ms)" font "Times-new-roman,21" offset .5,-0.2,0
	set tmargin 2.7
	set lmargin 5.5
	set encoding utf8
	set rmargin 2
	set bmargin 3
	#set logscale x
	set xtics 50
	set yrange [0:]
    set xrange [0:]
	set ytics 1
	#set xrange[:100]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	#set ytics 0,200,800 scale 0.4
	#set xtics (1,3,5,7,10) scale 0.4
    set key outside top center font "Times-new-roman,19" samplen 3 maxrows 2 spacing 1 width -4
	set style function linespoints
    plot "lat_tput_data_em" using (\$7/(1000)):(\$4/1000) title "Scalog (1.2ms)" with linespoints lc rgb 'coral' dashtype 3 lw 5 pt 4 ps 1.3,\
     "lat_tput_data_em" using (\$6/(1000)):(\$2/1000) title 'Belfast (1.2ms)' with linespoints lc rgb '#009988'  dashtype 1 lw 5 pt 3 ps 1.3,\
     "lat_tput_data_em" using (\$7/(1000)):(\$5/1000) title "Scalog (2ms)" with linespoints lc rgb 'coral' dashtype 5 lw 5 pt 4 ps 1.3,\
     "lat_tput_data_em" using (\$6/(1000)):(\$3/1000) title 'Belfast (2ms)' with linespoints lc rgb '#009988'  dashtype 2 lw 5 pt 3 ps 1.3


EOF
) | gnuplot -persist
"""

subprocess.run(['bash'], input=gnuplot_lat, text=True)
subprocess.run(['bash'], input=gnuplot_tput, text=True)
subprocess.run(['bash'], input=gnuplot_cdf(1), text=True)
subprocess.run(['bash'], input=gnuplot_cdf(2), text=True)
subprocess.run(['bash'], input=gnuplot_cdf(3), text=True)
subprocess.run(['bash'], input=gnuplot_lat_tput, text=True)
subprocess.run(['bash'], input="epstopdf lat_tput_em.eps", text=True)
subprocess.run(['bash'], input="epstopdf scalee2e.eps", text=True)
subprocess.run(['bash'], input="epstopdf scaletput.eps", text=True)
subprocess.run(['bash'], input=f"epstopdf scalecdf_{1}.eps", text=True)
subprocess.run(['bash'], input=f"epstopdf scalecdf_{2}.eps", text=True)
subprocess.run(['bash'], input=f"epstopdf scalecdf_{3}.eps", text=True)
subprocess.run(['bash'], input="rm scalee2e.eps scaletput.eps scalecdf*.eps data lat_tput_data_em lat_tput_em.eps lat_data cdfdata*", text=True)
subprocess.run(['bash'], input="mv scaletput.pdf 14a.pdf", text=True)
subprocess.run(['bash'], input="mv scalee2e.pdf 14b.pdf", text=True)
subprocess.run(['bash'], input=f"mv scalecdf_{1}.pdf 14c-sample{1}.pdf", text=True)
subprocess.run(['bash'], input=f"mv scalecdf_{2}.pdf 14c-sample{2}.pdf", text=True)
subprocess.run(['bash'], input=f"mv scalecdf_{3}.pdf 14c-sample{3}.pdf", text=True)
subprocess.run(['bash'], input="mv lat_tput_em.pdf 14d.pdf", text=True)
