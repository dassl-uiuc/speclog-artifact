import subprocess

output = subprocess.run(["python3", "helper_tput.py"], capture_output=True, text=True)
shard_failure = None 
view_change = None 
failure_detected = None
for line in output.stdout.splitlines():
    if "shard failure:" in line:
        shard_failure = line.split(":")[-1].strip()
    if "view change notified:" in line:
        view_change = line.split(":")[-1].strip()
    if "shard failure detected" in line:
        failure_detected = line.split(":")[-1].strip()
print(f"shard_failure: {shard_failure}")
print(f"view_change: {view_change}")
print(f"failure_detected: {failure_detected}")

zoomstart = int(float(shard_failure))
zoomend = zoomstart + 1

gnuplot_lat = rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 4, 1.4 font "Times-new-roman,19"
	set output "fail_e2e.eps"
	set xlabel "Time (s)" font "Times-new-roman,22" offset 0,0.4,0
	set ylabel "E2E Latency (ms)" font "Times-new-roman,22" offset 1,-0.2,0
	set tmargin 0.5
	set lmargin 6.3
	set encoding utf8
	set rmargin 17
	set bmargin 3
	set yrange [0:72]
	#set xrange [0:17]
	set tics scale 0.4
	#set ytics 0,200,800 scale 0.4
	set xtics 0.2 scale 0.4
	set key outside right center font "Times-new-roman,19" samplen 2 maxrows 4 width -2
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

gnuplot_tput = rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 4, 1.4 font "Times-new-roman,19"
	set output "fail_tput.eps"
	set xlabel "Time (s)" font "Times-new-roman,22" offset 0,0.4,0
	set ylabel "Throughput (KOps/s)" font "Times-new-roman,22" offset 1,-1,0
	set tmargin 0.5
	set lmargin 5.5
	set encoding utf8
	set rmargin 17
	set bmargin 3
	set yrange [0:18]
	set xrange [{zoomstart}:{zoomend}]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	set tics scale 0.4
	set ytics 4 scale 0.4
	set xtics 0.2 scale 0.4
	set key outside right center font "Times-new-roman,20" samplen 3 maxrows 4 width -2
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

subprocess.run(["bash"], input=gnuplot_lat, text=True)
subprocess.run(["bash"], input=gnuplot_tput, text=True)
subprocess.run(["bash"], input="epstopdf fail_e2e.eps", text=True)
subprocess.run(["bash"], input="epstopdf fail_tput.eps", text=True)
subprocess.run(["bash"], input="rm fail_e2e.eps fail_tput.eps e2elat tput", text=True)
subprocess.run(["bash"], input="mv fail_e2e.pdf 14a.pdf", text=True)
subprocess.run(["bash"], input="mv fail_tput.pdf 14b.pdf", text=True)