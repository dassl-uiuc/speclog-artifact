import numpy as np
import glob
import pandas as pd
import subprocess


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

    compute_time = int(path.split("_")[-1].split("/")[0])

    df.loc[compute_time] = {
        'mean_delivery_latency': np.mean(delivery_latency_array),
        'mean_e2e_latency': np.mean(e2e_latency_array),
        'p99_e2e_latency': np.percentile(e2e_latency_array, 99)
    }

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

    compute_time = int(path.split("_")[-1].split("/")[0])
    df.loc[compute_time] = {
        'mean_delivery_latency': np.mean(delivery_latency_array),
        'mean_e2e_latency': np.mean(e2e_latency_array),
        'p99_e2e_latency': np.percentile(e2e_latency_array, 99)
    }

    return df


df_e2e_speclog_2shard = pd.DataFrame(columns=[
    'mean_delivery_latency', 'mean_e2e_latency', 'p99_e2e_latency'
])

df_e2e_scalog_2shard = pd.DataFrame(columns=[
    'mean_delivery_latency', 'mean_e2e_latency', 'p99_e2e_latency'
])

df_e2e_speclog_4shard = pd.DataFrame(columns=[
    'mean_delivery_latency', 'mean_e2e_latency', 'p99_e2e_latency'
])

df_e2e_scalog_4shard = pd.DataFrame(columns=[
    'mean_delivery_latency', 'mean_e2e_latency', 'p99_e2e_latency'
])

for path in glob.glob("/users/sgbhat3/data/speclog/e2e_2shard/e2e_1500"):
    df_e2e_speclog_2shard = get_e2e_metrics_speclog(path + "/", df_e2e_speclog_2shard)

for path in glob.glob("/users/sgbhat3/data/scalog/e2e_2shard/e2e_1500"): 
    df_e2e_scalog_2shard = get_e2e_metrics_scalog(path + "/", df_e2e_scalog_2shard)

for path in glob.glob("/users/sgbhat3/data/speclog/e2e_4shard/e2e_1500"):
    df_e2e_speclog_4shard = get_e2e_metrics_speclog(path + "/", df_e2e_speclog_4shard)

for path in glob.glob("/users/sgbhat3/data/scalog/e2e_4shard/e2e_1500"):
    df_e2e_scalog_4shard = get_e2e_metrics_scalog(path + "/", df_e2e_scalog_4shard)

print(df_e2e_scalog_2shard)
print(df_e2e_scalog_4shard)
print(df_e2e_speclog_2shard)
print(df_e2e_speclog_4shard)

with open("rq1_speclog.dat", "w") as f:
    f.write(f"\"2-shards\" {round(df_e2e_speclog_2shard.loc[1500, 'mean_e2e_latency'], 2)} {round(df_e2e_speclog_2shard.loc[1500, 'p99_e2e_latency'], 2)} 0 {round(float(df_e2e_scalog_2shard.loc[1500, 'mean_e2e_latency'])/df_e2e_speclog_2shard.loc[1500, 'mean_e2e_latency'], 2)}X {round(df_e2e_speclog_2shard.loc[1500, 'mean_delivery_latency'], 2)} {round(float(df_e2e_scalog_2shard.loc[1500, 'mean_delivery_latency'])/df_e2e_speclog_2shard.loc[1500, 'mean_delivery_latency'], 2)}X\n")
    f.write(f"\"4-shards\" {round(df_e2e_speclog_4shard.loc[1500, 'mean_e2e_latency'], 2)} {round(df_e2e_speclog_4shard.loc[1500, 'p99_e2e_latency'], 2)} 1 {round(float(df_e2e_scalog_4shard.loc[1500, 'mean_e2e_latency'])/df_e2e_speclog_4shard.loc[1500, 'mean_e2e_latency'], 2)}X {round(df_e2e_speclog_4shard.loc[1500, 'mean_delivery_latency'], 2)} {round(float(df_e2e_scalog_4shard.loc[1500, 'mean_delivery_latency'])/df_e2e_speclog_4shard.loc[1500, 'mean_delivery_latency'], 2)}X\n")

with open("rq1_scalog.dat", "w") as f:
    f.write(f"2 {round(df_e2e_scalog_2shard.loc[1500, 'mean_e2e_latency'], 2)} {round(df_e2e_scalog_2shard.loc[1500, 'p99_e2e_latency'], 2)} {round(df_e2e_scalog_2shard.loc[1500, 'mean_delivery_latency'], 2)}\n")
    f.write(f"4 {round(df_e2e_scalog_4shard.loc[1500, 'mean_e2e_latency'], 2)} {round(df_e2e_scalog_4shard.loc[1500, 'p99_e2e_latency'], 2)} {round(df_e2e_scalog_4shard.loc[1500, 'mean_delivery_latency'], 2)}\n")


# run plotting scripts
gnuplot_script_delivery=r"""
#! /bin/bash

(cat <<EOF
set terminal postscript eps enhanced color size 2, 1.6 font "Times-new-roman,23"
set output 'rq1-dlat.eps'

#set yrange [0:20]
set style line 2 lc rgb 'black' lt 1 lw 2
set style data histogram
set style histogram cluster gap 1
set style fill pattern border 0
set xtics format "" scale 0
set boxwidth 0.975
set ytics scale 0.5
set ytics 2
#set format y "10^{%T}"
set xrange [-0.6:1.6]
set yrange [0:4.2]
set tmargin 1.6
set lmargin 5
set rmargin 1
set bmargin 2.5

set label '\\@20K' at 0,-1.5 center 
set label '\\@40K' at 1,-1.5 center 

set key at 1.8,5.1 font "Times-new-roman,21" samplen 2.5 maxrows 1 width -2
set ylabel "Delivery Latency (ms)" font "Times-new-roman,23" offset 0.5,0,0
plot 'rq1_scalog.dat' using (\$4/1000):xtic(1) title "Scalog" fs pattern 7 border rgb "coral" lc rgb 'coral' lw 2,\
 	 'rq1_speclog.dat' using (\$6/1000):xtic(1) title "Belfast" fs pattern 3 border rgb "#009988" lc rgb '#009988' lw 2,\
 	 'rq1_speclog.dat' using (\$4+0.27):(\$6/1000+0.5):7 title "" with labels center font "Times-new-roman,21"

EOF
) | gnuplot -persist
"""

gnuplot_script_e2e=r"""
#! /bin/bash

(cat <<EOF
set terminal postscript eps enhanced color size 2, 1.6 font "Times-new-roman,23"
set output 'rq1-e2elat.eps'

#set yrange [0:20]
set style line 2 lc rgb 'black' lt 1 lw 2
set style data histogram
set style histogram cluster gap 1
set style fill pattern border 0
set xtics format "" scale 0
set boxwidth 0.975
set ytics scale 0.5
set ytics 2
#set format y "10^{%T}"
set xrange [-0.6:1.6]
set yrange [0:7]
set tmargin 1.6
set lmargin 5
set rmargin 1
set bmargin 2.5

set label '\\@20K' at 0,-2.5 center 
set label '\\@40K' at 1,-2.5 center 

#BB5566
#004488
#77AADD
set key at 1.8,8.5 font "Times-new-roman,21" samplen 2.5 maxrows 1 width -2
set ylabel "E2E Latency (ms)" font "Times-new-roman,23" offset 0.5,0,0
plot 'rq1_scalog.dat' using (\$2/1000):xtic(1) title "Scalog" fs pattern 7 border rgb "coral" lc rgb 'coral' lw 2,\
 	 'rq1_speclog.dat' using (\$2/1000):xtic(1) title "Belfast" fs pattern 3 border rgb "#009988" lc rgb '#009988' lw 2,\
 	 'rq1_speclog.dat' using (\$4+0.27):(\$2/1000+0.5):5 title "" with labels center font "Times-new-roman,21"

EOF
) | gnuplot -persist
"""

subprocess.run(['bash'], input=gnuplot_script_delivery, text=True)
subprocess.run(['bash'], input=gnuplot_script_e2e, text=True)
subprocess.run(['bash'], input="epstopdf rq1-dlat.eps", text=True)
subprocess.run(['bash'], input="epstopdf rq1-e2elat.eps", text=True)
subprocess.run(['bash'], input="rm *.eps *.dat", text=True)
subprocess.run(['bash'], input="mv rq1-dlat.pdf 6a.pdf", text=True)
subprocess.run(['bash'], input="mv rq1-e2elat.pdf 6b.pdf", text=True)




