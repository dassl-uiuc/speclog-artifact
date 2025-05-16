import subprocess

output = subprocess.run(["python3", "helper_tput.py"], capture_output=True, text=True)
output_mask = subprocess.run(["python3", "helper_tput_mask.py"], capture_output=True, text=True)
shard_failure = None 
view_change = None 
failure_detected = None
app_resume = None
for line in output.stdout.splitlines():
    if "shard failure:" in line:
        shard_failure = line.split(":")[-1].strip()
    if "view change notified:" in line:
        view_change = line.split(":")[-1].strip()
    if "shard failure detected" in line:
        failure_detected = line.split(":")[-1].strip()
    if "app resume" in line:
        app_resume = line.split(":")[-1].strip()

for line in output_mask.stdout.splitlines():
    if "replica failure:" in line:
        replica_failure = line.split(":")[-1].strip()

print(f"shard_failure: {shard_failure}")
print(f"view_change: {view_change}")
print(f"failure_detected: {failure_detected}")
print(f"app_resume: {app_resume}")
print(f"replica_failure: {replica_failure}")
diff = float(shard_failure) - float(replica_failure)

zoomstart = int(float(shard_failure))
zoomend = zoomstart + 1

gnuplot_lat_shard = rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 4, 1.4 font "Times-new-roman,19"
	set output "fail_e2e_shard.eps"
	set xlabel "Time (s)" font "Times-new-roman,22" offset 0,0.4,0
	set ylabel "E2E Latency (ms)" font "Times-new-roman,22" offset 1,-0.2,0
	set tmargin 0.5
	set lmargin 6.3
	set encoding utf8
	set rmargin 18
	set bmargin 3
	set yrange [0:72]
	#set xrange [0:17]
	set xrange [{zoomstart}:{zoomend}]
	set tics scale 0.4
	#set ytics 0,200,800 scale 0.4
	set xtics 0.2 scale 0.4
	set key outside right center font "Times-new-roman,18" samplen 2 maxrows 5 width -2
	set style function linespoints
	set trange [0:100]
	set parametric
	plot {shard_failure},t title "Shard Fail" dt 3 lw 5 ps 0 lc rgb 'gray',\
		 {failure_detected},t title "Detected" dt 1 lw 5 ps 0 lc rgb 'coral',\
		 {view_change},t title "View change" dt 2 lw 5 ps 0  lc rgb 'black',\
	     "e2elat" using 1:2 title 'E2E Latency' with lines lc rgb '#009988' dashtype 1 lw 5
		
EOF
) | gnuplot -persist
"""

gnuplot_tput_shard = rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 4, 1.4 font "Times-new-roman,19"
	set output "fail_tput_shard.eps"
	set xlabel "Time (s)" font "Times-new-roman,22" offset 0,0.4,0
	set ylabel "Throughput (KOps/s)" font "Times-new-roman,22" offset 1,-1,0
	set tmargin 0.5
	set lmargin 5.5
	set encoding utf8
	set rmargin 18.3
	set bmargin 3
	set yrange [0:18]
	set xrange [{zoomstart}:{zoomend}]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	set tics scale 0.4
	set ytics 4 scale 0.4
	set xtics 0.2 scale 0.4
	set key outside right center font "Times-new-roman,18" samplen 3 maxrows 5 width -2
	set style function linespoints

	set trange [0:20]
	set parametric
	plot {shard_failure},t title "Shard Fail" dt 3 lw 5 ps 0 lc rgb 'gray',\
		 {failure_detected},t title "Detected" dt 1 lw 5 ps 0 lc rgb 'coral',\
		 {view_change},t title "View change" dt 2 lw 5 ps 0  lc rgb 'black',\
		 "tput" using 1:(\$2/1000) title 'Throughput' with lines lc rgb '#009988'  dashtype 1 lw 5
EOF
) | gnuplot -persist
"""

gnuplot_lat_replica = rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 4, 1.4 font "Times-new-roman,19"
	set output "fail_e2e_replica.eps"
	set xlabel "Time (s)" font "Times-new-roman,22" offset 0,0.4,0
	set ylabel "E2E Latency (ms)" font "Times-new-roman,22" offset 1,-0.2,0
	set tmargin 0.5
	set lmargin 6.3
	set encoding utf8
	set rmargin 18
	set bmargin 3
	set yrange [0:72]
	#set xrange [0:17]
	set xrange [{zoomstart}:{zoomend}]
	set tics scale 0.4
	#set ytics 0,200,800 scale 0.4
	set xtics 0.2 scale 0.4
	set key outside right center font "Times-new-roman,18" samplen 2 maxrows 5 width -2
	set style function linespoints
	set trange [0:100]
	set parametric
	plot {replica_failure}+{diff},t title "Replica Fail" dt 3 lw 5 ps 0 lc rgb 'gray',\
	     "e2elat_mask" using (\$1+{diff}):2 title 'E2E Latency' with lines lc rgb '#009988' dashtype 1 lw 5
		
EOF
) | gnuplot -persist
"""

gnuplot_tput_replica = rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 4, 1.4 font "Times-new-roman,19"
	set output "fail_tput_replica.eps"
	set xlabel "Time (s)" font "Times-new-roman,22" offset 0,0.4,0
	set ylabel "Throughput (KOps/s)" font "Times-new-roman,22" offset 1,-1,0
	set tmargin 0.5
	set lmargin 5.5
	set encoding utf8
	set rmargin 18.3
	set bmargin 3
	set yrange [0:18]
	set xrange [{zoomstart}:{zoomend}]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	set tics scale 0.4
	set ytics 4 scale 0.4
	set xtics 0.2 scale 0.4
	set key outside right center font "Times-new-roman,18" samplen 3 maxrows 5 width -2
	set style function linespoints

	set trange [0:20]
	set parametric
	plot {replica_failure}+{diff},t title "Replica Fail" dt 3 lw 5 ps 0 lc rgb 'gray',\
		 "tput_mask" using (\$1+{diff}):(\$2/1000) title 'Throughput' with lines lc rgb '#009988'  dashtype 1 lw 5
EOF
) | gnuplot -persist
"""

gnuplot_breakdown=rf"""
#! /bin/bash
# We are plotting columns 2, 3 and 4 as y-values,
# the x-ticks are coming from column 1


(cat <<EOF
set terminal postscript eps enhanced color size 2, 1.7 font "Times-new-roman,20"
set output 'breakdown.eps'

set style line 2 lc rgb 'black' lt 1 lw 2
set style data histogram
set style histogram rowstacked
set style fill pattern border 0
set xtics format "" scale 0 offset 0,.3 font "Times-new-roman,19"
set boxwidth 0.4
set yrange[0:60]
set xrange [-0.6:.6]
set ytics 10
set ylabel "Time (ms)" font "Times-new-roman,23" offset 1,0,0
set lmargin 6
set rmargin 4
set tmargin 3
set key outside top center font "Times-new-roman,19" samplen 2.5 maxrows 2 width -5
plot 'breakdown' using (\$2*1000) title 'detect' fs pattern 3 border rgb "black" lc rgb 'gray60' lw 2,\
    '' using (\$4*1000) title 'view change' fs pattern 7 lw 2,\
    '' using (\$6*1000) title 'rollback' fs pattern 6 lw 2

EOF
) | gnuplot -persist
"""

subprocess.run(["bash"], input=gnuplot_lat_shard, text=True)
subprocess.run(["bash"], input=gnuplot_tput_shard, text=True)
subprocess.run(["bash"], input=gnuplot_lat_replica, text=True)
subprocess.run(["bash"], input=gnuplot_tput_replica, text=True)
subprocess.run(["bash"], input=gnuplot_breakdown, text=True)
subprocess.run(["bash"], input="epstopdf fail_e2e_shard.eps", text=True)
subprocess.run(["bash"], input="epstopdf fail_tput_shard.eps", text=True)
subprocess.run(["bash"], input="epstopdf fail_e2e_replica.eps", text=True)
subprocess.run(["bash"], input="epstopdf fail_tput_replica.eps", text=True)
subprocess.run(["bash"], input="epstopdf breakdown.eps", text=True)
subprocess.run(["bash"], input="rm breakdown.eps breakdown", text=True)
subprocess.run(["bash"], input="rm fail_e2e_shard.eps fail_tput_shard.eps fail_e2e_replica.eps fail_tput_replica.eps e2elat tput e2elat_mask tput_mask", text=True)
subprocess.run(["bash"], input="mv fail_e2e_shard.pdf app_failure_shard_e2e.pdf", text=True)
subprocess.run(["bash"], input="mv fail_tput_shard.pdf app_failure_shard_tput.pdf", text=True)
subprocess.run(["bash"], input="mv fail_e2e_replica.pdf app_failure_replica_e2e.pdf", text=True)
subprocess.run(["bash"], input="mv fail_tput_replica.pdf app_failure_replica_tput.pdf", text=True)
subprocess.run(["bash"], input="mv breakdown.pdf app_failure_breakdown.pdf", text=True)