import subprocess

#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

gnuplot_tput = r"""
(cat <<EOF
	set terminal postscript eps enhanced color size 2.3, 1.7 font "Times-new-roman,19"
	set output "qc_tput.eps"
	set xlabel "Time (s)" font "Times-new-roman,21" offset 0,0.4,0
	set ylabel "Throughput (KOps/s)" font "Times-new-roman,20" offset 1,-0.2,0
	set tmargin .5
	set lmargin 5.5
	set encoding utf8
	set rmargin .5
	set bmargin 3
	set yrange [0:21]
	set xrange [0:17]
	#set y2tics nomirror
	#set ytics nomirror
	#set y2range[1:]
	set tics scale 0.4
	#set ytics 0,200,800 scale 0.4
	set xtics 0,5,20 scale 0.4
	set key at 17,11 font "Times-new-roman,18" samplen 1.3 maxrows 5 width -4
	set style function linespoints
	set trange [0:22]
	set parametric
	plot "scalog-tput" using (\$1/1000):(\$2/1000) title 'Scalog' with lines lc rgb 'coral'  dashtype 1 lw 4,\
		"enabled-tput" using (\$1/1000):(\$2/1000) title 'Belfast R' with lines lc rgb 'black'  dashtype 1 lw 4,\
		"disabled-tput" using (\$1/1000):(\$2/1000) title 'Belfast R no-qc' with lines lc rgb 'orange'  dashtype 1 lw 4,\
		"enabled-tput" using (\$3/1000):(\$4/1000) title 'Belfast R+N' with lines lc rgb '#009988'  dashtype 1 lw 4,\
		"disabled-tput" using (\$3/1000):(\$4/1000) title 'Belfast R+N no-qc' with lines lc rgb '#FFD689'  dashtype 1 lw 4
		
		

EOF
) | gnuplot -persist
"""

gnuplot_lat = r"""
#! /bin/bash
#run this script to generate the lat vs thrpt graphs for c and w

#./plot_lat_thrpt.sh ../paper_results/wc-2 sync w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 async w 2;
#./plot_lat_thrpt.sh ../paper_results/wc-2 sync c 2;

(cat <<EOF
	set terminal postscript eps enhanced color size 2.3, 1.7 font "Times-new-roman,19"
	set output "qc_e2e.eps"
	set xlabel "Time (s)" font "Times-new-roman,21" offset 0,0.4,0
	set ylabel "E2E Latency (ms)" font "Times-new-roman,20" offset 1,-0.2,0
	set tmargin 0.5
	set lmargin 5
	set encoding utf8
	set rmargin .5
	set bmargin 3
	set yrange [0:5.5]
	set xrange [0:17]
	set tics scale 0.4
	#set ytics 0,200,800 scale 0.4
	set xtics 0,5,20 scale 0.4
	set key at 17,2.0 font "Times-new-roman,20" samplen 1.3 maxrows 4 width -2
	set style function linespoints
	plot "scalog-e2elat" using ((\$1-20064.2029)/1000):(\$2/1000) title 'Scalog' with lines lc rgb 'coral'  dashtype 1 lw 4,\
		"disabled-e2elat" using ((\$1-20020.986)/1000):(\$2/1000) title 'Belfast no-qc' with lines lc rgb '#FFD689'  dashtype 1 lw 4,\
		"enabled-e2elat" using ((\$1-20078.568)/1000):(\$2/1000) title 'Belfast' with lines lc rgb '#009988'  dashtype 1 lw 4
		
		
EOF
) | gnuplot -persist"""


subprocess.run(['python3', 'helper_tput_speclog.py', 'enabled'], capture_output=False, text=True)
subprocess.run(['python3', 'helper_tput_speclog.py', 'disabled'], capture_output=False, text=True)
subprocess.run(['python3', 'helper_tput_scalog.py'], capture_output=False, text=True)
subprocess.run(['python3', 'helper_lat_speclog.py', 'enabled'], capture_output=False, text=True)
subprocess.run(['python3', 'helper_lat_speclog.py', 'disabled'], capture_output=False, text=True)
subprocess.run(['python3', 'helper_lat_scalog.py'], capture_output=False, text=True)
subprocess.run(['bash'], input=gnuplot_lat, text=True)
subprocess.run(['bash'], input=gnuplot_tput, text=True)
subprocess.run(['bash'], input="epstopdf qc_e2e.eps", text=True)
subprocess.run(['bash'], input="epstopdf qc_tput.eps", text=True)
subprocess.run(['bash'], input="rm *.eps *-tput *-e2elat", text=True)
subprocess.run(['bash'], input="mv qc_tput.pdf 10a.pdf", text=True)
subprocess.run(['bash'], input="mv qc_e2e.pdf 10b.pdf", text=True)