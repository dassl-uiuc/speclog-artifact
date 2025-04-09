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

def get_splits_speclog(path, df):
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

    min_size = min(len(compute_latency_array), len(delivery_latency_array), 
                len(confirmation_latency_array), len(queuing_delay_array))

    compute_latency_array = compute_latency_array[:min_size]
    delivery_latency_array = delivery_latency_array[:min_size]
    confirmation_latency_array = confirmation_latency_array[:min_size]
    queuing_delay_array = queuing_delay_array[:min_size]

    compute = compute_latency_array - delivery_latency_array - queuing_delay_array
    wait_for_confirmation = confirmation_latency_array - compute_latency_array
    for i in range(len(wait_for_confirmation)):
        if wait_for_confirmation[i] < 0:
            wait_for_confirmation[i] = 0
        else:
            wait_for_confirmation[i] = wait_for_confirmation[i]


    compute_time = int(path.split("_")[-1].split("/")[0])
    df.loc[compute_time] = {
        'Delivery': np.mean(delivery_latency_array),
        'DownstreamCompute': np.mean(compute),
        'QueuingDelay': np.mean(queuing_delay_array),
        'WaitForConfirmation': np.mean(wait_for_confirmation),
        'sanitye2e': np.mean(delivery_latency_array) + np.mean(compute) + np.mean(queuing_delay_array) + np.mean(wait_for_confirmation)
    }
    return df

def get_splits_scalog(path, df):
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

    min_size = min(len(delivery_latency_array), len(e2e_latency_array), len(queuing_delay_array))

    delivery_latency_array = delivery_latency_array[:min_size]
    e2e_latency_array = e2e_latency_array[:min_size]
    queuing_delay_array = queuing_delay_array[:min_size]

    compute = e2e_latency_array - delivery_latency_array - queuing_delay_array

    compute_time = int(path.split("_")[-1].split("/")[0])
    df.loc[compute_time] = {
        'Delivery': np.mean(delivery_latency_array),
        'DownstreamCompute': np.mean(compute),
        'QueuingDelay': np.mean(queuing_delay_array),
        'sanitye2e': np.mean(delivery_latency_array) + np.mean(compute) + np.mean(queuing_delay_array)
    }
    return df


comp_lat = [100, 500, 1000, 1500, 2000, 2500, 3000, 4000, 5000]


df_splits_scalog_4shard = pd.DataFrame(columns=[
    'Delivery', 'DownstreamCompute', 'QueuingDelay', 'sanitye2e'
])

df_splits_speclog_4shard = pd.DataFrame(columns=[   
    'Delivery', 'DownstreamCompute', 'QueuingDelay', 'WaitForConfirmation', 
    'sanitye2e'
])

df_e2e_speclog_4shard = pd.DataFrame(columns=[
    'mean_delivery_latency', 'mean_e2e_latency', 'p99_e2e_latency'
])

df_e2e_scalog_4shard = pd.DataFrame(columns=[
    'mean_delivery_latency', 'mean_e2e_latency', 'p99_e2e_latency'
])

for path in glob.glob("/users/sgbhat3/data/speclog/e2e_4shard/*"):
    df_e2e_speclog_4shard = get_e2e_metrics_speclog(path + "/", df_e2e_speclog_4shard)
    df_splits_speclog_4shard = get_splits_speclog(path + "/", df_splits_speclog_4shard)

for path in glob.glob("/users/sgbhat3/data/scalog/e2e_4shard/*"):
    df_e2e_scalog_4shard = get_e2e_metrics_scalog(path + "/", df_e2e_scalog_4shard)
    df_splits_scalog_4shard = get_splits_scalog(path + "/", df_splits_scalog_4shard)

# print(df_e2e_scalog_4shard)
# print(df_e2e_speclog_4shard)
# print(df_splits_scalog_4shard)
# print(df_splits_speclog_4shard)
with open("stacked-data-4", "w") as f: 
    f.write("system computation_time Delivery DownstreamQueue DownstreamCompute WaitForConfirm sanitychecke2e\n")
    f.write("{} {} {} {} {} {} {} {} {}\n".format("S", 500, 
        df_splits_scalog_4shard.loc[500]['Delivery'],
        df_splits_scalog_4shard.loc[500]['QueuingDelay'],
        df_splits_scalog_4shard.loc[500]['DownstreamCompute'],
        0,
        df_splits_scalog_4shard.loc[500]['sanitye2e'], 
        "\"\\240\"", 0))
    f.write("{} {} {} {} {} {} {} {}X {}\n".format("B", 500,
        df_splits_speclog_4shard.loc[500]['Delivery'],
        df_splits_speclog_4shard.loc[500]['QueuingDelay'],
        df_splits_speclog_4shard.loc[500]['DownstreamCompute'],
        df_splits_speclog_4shard.loc[500]['WaitForConfirmation'],
        df_splits_speclog_4shard.loc[500]['sanitye2e'], 
        round(df_splits_scalog_4shard.loc[500]['sanitye2e']/df_splits_speclog_4shard.loc[500]['sanitye2e'], 2), 1))
    f.write("\"\\240\" 	0	0	0	0	0\n")
    f.write("{} {} {} {} {} {} {} {} {}\n".format("S", 1000,
        df_splits_scalog_4shard.loc[1000]['Delivery'],
        df_splits_scalog_4shard.loc[1000]['QueuingDelay'],
        df_splits_scalog_4shard.loc[1000]['DownstreamCompute'],
        0,
        df_splits_scalog_4shard.loc[1000]['sanitye2e'], 
        "\"\\240\"", 0))
    f.write("{} {} {} {} {} {} {} {}X {}\n".format("B", 1000,
        df_splits_speclog_4shard.loc[1000]['Delivery'],
        df_splits_speclog_4shard.loc[1000]['QueuingDelay'],
        df_splits_speclog_4shard.loc[1000]['DownstreamCompute'],
        df_splits_speclog_4shard.loc[1000]['WaitForConfirmation'],
        df_splits_speclog_4shard.loc[1000]['sanitye2e'],
        round(df_splits_scalog_4shard.loc[1000]['sanitye2e']/df_splits_speclog_4shard.loc[1000]['sanitye2e'], 2), 4))
    f.write("\"\\240\" 	0	0	0	0	0\n") 
    f.write("{} {} {} {} {} {} {} {} {}\n".format("S", 1500,
        df_splits_scalog_4shard.loc[1500]['Delivery'],
        df_splits_scalog_4shard.loc[1500]['QueuingDelay'],
        df_splits_scalog_4shard.loc[1500]['DownstreamCompute'],
        0,
        df_splits_scalog_4shard.loc[1500]['sanitye2e'], 
        "\"\\240\"", 0))
    f.write("{} {} {} {} {} {} {} {}X {}\n".format("B", 1500,
        df_splits_speclog_4shard.loc[1500]['Delivery'],
        df_splits_speclog_4shard.loc[1500]['QueuingDelay'],
        df_splits_speclog_4shard.loc[1500]['DownstreamCompute'],
        df_splits_speclog_4shard.loc[1500]['WaitForConfirmation'],
        df_splits_speclog_4shard.loc[1500]['sanitye2e'], 
        round(df_splits_scalog_4shard.loc[1500]['sanitye2e']/df_splits_speclog_4shard.loc[1500]['sanitye2e'], 2), 7))
    f.write("\"\\240\" 	0	0	0	0	0\n")
    f.write("{} {} {} {} {} {} {} {} {}\n".format("S", 2000,
        df_splits_scalog_4shard.loc[2000]['Delivery'],
        df_splits_scalog_4shard.loc[2000]['QueuingDelay'],
        df_splits_scalog_4shard.loc[2000]['DownstreamCompute'],
        0,
        df_splits_scalog_4shard.loc[2000]['sanitye2e'], 
        "\"\\240\"", 0))
    f.write("{} {} {} {} {} {} {} {}X {}\n".format("B", 2000,
        df_splits_speclog_4shard.loc[2000]['Delivery'],
        df_splits_speclog_4shard.loc[2000]['QueuingDelay'],
        df_splits_speclog_4shard.loc[2000]['DownstreamCompute'],
        df_splits_speclog_4shard.loc[2000]['WaitForConfirmation'],
        df_splits_speclog_4shard.loc[2000]['sanitye2e'], 
        round(df_splits_scalog_4shard.loc[2000]['sanitye2e']/df_splits_speclog_4shard.loc[2000]['sanitye2e'], 2), 10))
    f.write("\"\\240\" 	0	0	0	0	0\n")
    f.write("{} {} {} {} {} {} {} {} {}\n".format("S", 4000,
        df_splits_scalog_4shard.loc[4000]['Delivery'],
        df_splits_scalog_4shard.loc[4000]['QueuingDelay'],
        df_splits_scalog_4shard.loc[4000]['DownstreamCompute'],
        0,
        df_splits_scalog_4shard.loc[4000]['sanitye2e'], 
        "\"\\240\"", 0))
    f.write("{} {} {} {} {} {} {} {}X {}\n".format("B", 4000,
        df_splits_speclog_4shard.loc[4000]['Delivery'],
        df_splits_speclog_4shard.loc[4000]['QueuingDelay'],
        df_splits_speclog_4shard.loc[4000]['DownstreamCompute'],
        df_splits_speclog_4shard.loc[4000]['WaitForConfirmation'],
        df_splits_speclog_4shard.loc[4000]['sanitye2e'], 
        round(float(df_splits_scalog_4shard.loc[4000]['sanitye2e'])/df_splits_speclog_4shard.loc[4000]['sanitye2e'], 2), 13))

    
with open("speclog-4shard", "w") as f:
    f.write("#computation_time e2e_latency ratio\n")
    for comp in comp_lat:
        f.write("{} {} {}X\n".format(comp, df_e2e_speclog_4shard.loc[comp]['mean_e2e_latency'], round(df_e2e_scalog_4shard.loc[comp]['mean_e2e_latency']/df_e2e_speclog_4shard.loc[comp]['mean_e2e_latency'], 2)))

with open("scalog-4shard", "w") as f:
    f.write("#computation_time e2e_latency ratio\n")
    for comp in comp_lat:
        f.write("{} {} {}X\n".format(comp, df_e2e_scalog_4shard.loc[comp]['mean_e2e_latency'], round(df_e2e_scalog_4shard.loc[comp]['mean_e2e_latency']/df_e2e_speclog_4shard.loc[comp]['mean_e2e_latency'], 2)))


gnuplot_script_e2e = r"""

(cat <<EOF
	set terminal postscript eps enhanced color size 2.1, 1.5 font "Times-new-roman,19"
	set output "e2evscompute.eps"
	set xlabel "Compute Time (ms)" font "Times-new-roman,21" offset 0,0.7,0
	set ylabel "E2E Latency (ms)" font "Times-new-roman,20" offset 1,-0.2,0
	set tmargin .5
	set lmargin 6
	set encoding utf8
	set rmargin 1
	set bmargin 2.5
	set yrange [0:]
	set xrange [0:5.2]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	#set ytics 0,200,800 scale 0.4
	#set xtics (1,3,5,7,10) scale 0.4
	set key at 5,4 font "Times-new-roman,20" samplen 2.5 maxrows 2
	set style function linespoints
	plot "scalog-4shard" using (\$1/1000):(\$2/1000) title "Scalog" with linespoints lc rgb 'coral' dashtype 3 lw 5 pt 4 ps 1.3,\
		"speclog-4shard" using  (\$1/1000):(\$2/1000) title 'Belfast' with linespoints lc rgb '#009988'  dashtype 1 lw 5 pt 3 ps 1.3,\
		#"scalog-2shard" using (\$1/1000):(\$3/1000) title "Scalog" with linespoints lc rgb 'mediumpurple3' dashtype 1 lw 5 pt 4 ps 1.3,\
		#"speclog-2shard" using  (\$1/1000):(\$3/1000) title 'Speclog' with linespoints lc rgb 'sea-green'  dashtype 3 lw 5 pt 3 ps 1.3,\
		#"speclog-2shard" using  (\$1/1000):8 title 'Speclog' with linespoints lc rgb 'sea-green'  dashtype 3 lw 5 pt 3 ps 1.3 axes x1y2
		#"speclog-2shard" using (\$1/1000):(\$6/1000+0.4):8 title "" with labels center font "Times-new-roman,19"
		#"scalog-4shard" using (\$1/1000):(\$4/1000) title "Scalog-4 shards" with linespoints lc rgb 'mediumpurple3' dashtype 1 lw 5 pt 4 ps 1.3,\
		#"speclog-4shard" using  (\$1/1000):(\$6/1000) title "Erwin-st 8K" with linespoints lc rgb 'sea-green'  dashtype 3 lw 5 pt 4 ps 1.3
		
EOF
) | gnuplot -persist
"""

gnuplot_script_benefit = r"""
#! /bin/bash

(cat <<EOF
	set terminal postscript eps enhanced color size 2.1, 1.5 font "Times-new-roman,19"
	set output "benefitratio.eps"
	set xlabel "Compute Time (ms)" font "Times-new-roman,21" offset 0,0.4,0
	set ylabel "Benefit Ratio" font "Times-new-roman,20" offset 1,-0.2,0
	set tmargin .5
	set lmargin 6.5
	set encoding utf8
	set rmargin 1
	set bmargin 2.75
	set yrange [1:]
	set xrange [0:5.2]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	#set ytics 0,200,800 scale 0.4
	#set xtics (1,3,5,7,10) scale 0.4
	set key at 5,1.7 font "Times-new-roman,20" samplen 2.5 maxrows 1
	set style function linespoints
	plot "speclog-4shard" using  (\$1/1000):3 title 'Belfast' with linespoints lc rgb '#009988'  dashtype 1 lw 5 pt 3 ps 1.3,\
		#"scalog-2shard" using (\$1/1000):(\$3/1000) title "Scalog" with linespoints lc rgb 'mediumpurple3' dashtype 1 lw 5 pt 4 ps 1.3,\
		#"speclog-2shard" using  (\$1/1000):(\$3/1000) title 'Speclog' with linespoints lc rgb 'sea-green'  dashtype 3 lw 5 pt 3 ps 1.3,\
		#"speclog-2shard" using  (\$1/1000):8 title 'Speclog' with linespoints lc rgb 'sea-green'  dashtype 3 lw 5 pt 3 ps 1.3 axes x1y2
		#"speclog-2shard" using (\$1/1000):(\$6/1000+0.4):8 title "" with labels center font "Times-new-roman,19"
		#"scalog-4shard" using (\$1/1000):(\$4/1000) title "Scalog-4 shards" with linespoints lc rgb 'mediumpurple3' dashtype 1 lw 5 pt 4 ps 1.3,\
		#"speclog-4shard" using  (\$1/1000):(\$6/1000) title "Erwin-st 8K" with linespoints lc rgb 'sea-green'  dashtype 3 lw 5 pt 4 ps 1.3
		
EOF
) | gnuplot -persist
"""




gnuplot_script_split=r"""
(cat <<EOF
	set terminal postscript eps enhanced color size 2.3,1.3 font "Times-new-roman,17"
	set output "splitup.eps"
	#set xlabel "Value size (bytes)" font "Times-new-roman,16" offset 0,0.5,0
	set ylabel "E2E latency (ms)" font "Times-new-roman,16" offset 1.5,0,0
	set tmargin 2.5
	set lmargin 5
	set rmargin .6
	set bmargin 3.5
	set style data histogram
	set style histogram rowstacked
	set style fill solid border -1
	set boxwidth 0.55
	set border lw 0.25
	set tics scale 0.3
	set xtics scale 0
	set xtics offset 0,.3 font "Times-new-roman,18"
	#set xrange[0:10]
	set yrange[0:10]
	set key vertical at 5.5,13 center samplen 0.8 font "Times-new-roman,15" autotitle columnheader maxrows 2 width -3
	set border 3
	set tics nomirror
	set label 'S: Scalog' at  -0.6,8.45 font "Times-new-roman,16"
	set label 'B: Belfast' at  -0.6,6.85 font "Times-new-roman,16"
	set label '0.5ms' at 	-0.5,-3.3 font "Times-new-roman,16" 
	set label '1ms' at  2.7,-3.3 font "Times-new-roman,16" 
	set label '1.5ms' at  5.4,-3.3 font "Times-new-roman,16" 
	set label '2ms' at  8.8,-3.3 font "Times-new-roman,16" 	 
	set label '4ms' at  11.7,-3.3 font "Times-new-roman,16" 	 
	set label 'Compute Time (ms)' at  2.9,-5.75 font "Times-new-roman,16" 
    plot "stacked-data-4" using (\$3/1000):xtic(1) lt -1 fs solid 0.5, \
	    '' using (\$4/1000) lt -1 fs pattern 7, \
	    '' using (\$5/1000) lt -1 fs solid 0 ,\
	    '' using (\$6/1000) lt -1 fs solid 1 , \
	    '' using (\$9+0.2):(\$7/1000+0.9):8 title "" with labels center font "Times-new-roman,14"

	    
EOF
) | gnuplot -persist
"""


subprocess.run(['bash'], input=gnuplot_script_benefit, text=True)
subprocess.run(['bash'], input=gnuplot_script_e2e, text=True)
subprocess.run(['bash'], input=gnuplot_script_split, text=True)
subprocess.run(['bash'], input="epstopdf benefitratio.eps", text=True)
subprocess.run(['bash'], input="epstopdf e2evscompute.eps", text=True)
subprocess.run(['bash'], input="epstopdf splitup.eps", text=True)
subprocess.run(['bash'], input="rm *.eps stacked-data-4 speclog-4shard scalog-4shard", text=True)
subprocess.run(['bash'], input="mv splitup.pdf 7c.pdf", text=True)
subprocess.run(['bash'], input="mv benefitratio.pdf 7b.pdf", text=True)
subprocess.run(['bash'], input="mv e2evscompute.pdf 7a.pdf", text=True)





