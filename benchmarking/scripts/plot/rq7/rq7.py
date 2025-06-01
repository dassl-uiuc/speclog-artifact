import subprocess

speclog_tput = subprocess.run(["python3", "tput_speclog.py"], capture_output=True)
join, new_window, leave = None, None, None
for line in speclog_tput.stdout.decode("utf-8").splitlines():
    if "shard requests to join" in line:
        join = float(line.split("\t")[0])
    if "first cut committed from new shard" in line:
        new_window = float(line.split("\t")[0])
    if "shard requests to leave" in line:
        leave = float(line.split("\t")[0])
scalog_tput = subprocess.run(["python3", "tput_scalog.py"], capture_output=True)
subprocess.run(["python3", "lat_scalog.py", str(join*1000)])
subprocess.run(["python3", "lat_speclog.py", str(join*1000)])


gnuplot_tput = rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 2.4, 1.8 font "Times-new-roman,19" 
	#set multiplot
	set output "reconfig-tput.eps"
	set multiplot
	set xlabel "Time (s)" font "Times-new-roman,21" offset 0,0.4,0
	set ylabel "Throughput (KOps/s)" font "Times-new-roman,20" offset 1,-0.2,0
	set tmargin 3
	set lmargin 5.5
	set encoding utf8
	set rmargin .5
	set bmargin 3
	set yrange [0:11]
	set xrange [0:57]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	set tics scale 0.4
	#set ytics 0,200,800 scale 0.4
	#set xtics (1,3,5,7,10) scale 0.4
	set trange [-100:100]
	set parametric
	set key at 59.8,14.5 font "Times-new-roman,19" samplen 1.5 maxrows 2 width -5.5
	set style function linespoints
	set label "close-up" at 23,8.2
	plot {join},t title "Join" dt 1 lw 4 ps 0,\
		{new_window},t title "New Window" dt 3 lw 4 ps 0 lc rgb "black",\
		{leave},t title "Leave" dt 2 lw 4 ps 0 lc rgb "red",\
		"scalog_tput.csv" using 1:(\$2/1000) title 'Scalog' with lines lc rgb 'coral'  dashtype 1 lw 4,\
		"speclog_tput.csv" using 1:(\$2/1000) title 'Belfast' with lines lc rgb '#009988'  dashtype 1 lw 4
	


	unset xlabel
	unset ylabel
	unset object
	unset label
	unset tics
	unset arrow
	set border lw 0.5
	#set yrange [95:100]
	set xrange [13:13.5]
	set xtics 0.2 font "Times-new-roman,19" offset 0,0.2 nomirror
	set ytics 3 font "Times-new-roman,19" offset 0.65,0 nomirror
	set origin 0.26,0.13	
	set size 0.5,0.67
	#set object rectangle from screen 0.25,0.14 to screen 0.5,0.7 behind fillcolor rgb 'white' fillstyle solid noborder
	unset format x
	set tics scale 0.5
	set arrow heads filled from 365,99 to 215,99 lt 2 lw 2 lc rgb "black" front
	set label '1.7x' at 310,98 font "Times-new-roman,20" front
	#set label '5%' at 225, 97 font "Times-new-roman,20" front 


	unset key
	plot {join},t title "Shard Join" dt 1 lw 2 ps 0,\
		{new_window},t title "New Window" dt 3 lw 3 ps 0 lc rgb "black",\
		"scalog_tput.csv" using 1:(\$2/1000) title 'Scalog' with lines lc rgb 'coral'  dashtype 1 lw 3,\
		"speclog_tput.csv" using 1:(\$2/1000) title 'Speclog' with lines lc rgb '#009988'  dashtype 1 lw 3

	unset multiplot
	
EOF
) | gnuplot -persist"""


gnuplot_lat = rf"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 2.1, 1.7 font "Times-new-roman,19" 
	set output "reconfig-e2e.eps"
	set xlabel "Time (s)" font "Times-new-roman,21" offset -0.2,0.4,0
	set ylabel "E2E Latency (ms)" font "Times-new-roman,20" offset 1,-0.2,0
	set tmargin 2.65
	set lmargin 5
	set encoding utf8
	set rmargin .5
	set bmargin 3
	set yrange [0:7]
	set xrange [0:57]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	set tics scale 0.4
	#set ytics 0,200,800 scale 0.4
	#set xtics (1,3,5,7,10) scale 0.4
	set trange [-100:100]
	set parametric
	set key at 56,9 font "Times-new-roman,19" samplen 1.5 maxrows 2 
	set style function linespoints
	plot {join},t title "Join" dt 1 lw 4 ps 0,\
		{leave},t title "Leave" dt 2 lw 4 ps 0 lc rgb "red",\
		"scalog_lat.csv" using (\$1/1000):(\$2/1000) title 'Scalog' with lines lc rgb 'coral'  dashtype 1 lw 4,\
		"speclog_lat.csv" using (\$1/1000):(\$2/1000) title 'Belfast' with lines lc rgb '#009988'  dashtype 1 lw 4
	


	
EOF
) | gnuplot -persist"""

subprocess.run(['bash'], input=gnuplot_lat, text=True)
subprocess.run(['bash'], input=gnuplot_tput, text=True)
subprocess.run(['bash'], input="epstopdf reconfig-tput.eps", text=True)
subprocess.run(['bash'], input="epstopdf reconfig-e2e.eps", text=True)
subprocess.run(['bash'], input="rm *.eps *.csv", text=True)
subprocess.run(['bash'], input="mv reconfig-tput.pdf 13a.pdf", text=True)
subprocess.run(['bash'], input="mv reconfig-e2e.pdf 13b.pdf", text=True)