import subprocess

subprocess.run(["python3", "analyze_id.py", "../../../results/apps/intrusion_detection/"], check=True)
subprocess.run(["python3", "analyze_ta.py", "../../../results/apps/transaction_analysis/"], check=True)
subprocess.run(["python3", "analyze_hft.py", "../../../results/apps/hft/"], check=True)


gnuplot = rf"""
#! /bin/bash

(cat <<EOF
set terminal postscript eps enhanced color size 2.5, 1.6 font "Times-new-roman,20"
set output 'apps.eps'

#set yrange [0:20]
set style line 2 lc rgb 'black' lt 1 lw 2
set style data histogram
set style histogram cluster gap 2
set style fill pattern border 0
set xtics format "" scale 0
set boxwidth 0.975
set ytics scale 0.5
set ytics 2
set xrange [-0.5:2.5]
set yrange [0:8]
set tmargin 1.7
set lmargin 5
set rmargin 2
set bmargin 2.5

#BB5566
#004488
#77AADD
set key at 2.8,9.5 font "Times-new-roman,21" samplen 2.5 maxrows 1
set ylabel "E2E Latency (ms)" font "Times-new-roman,21" offset 0.5,0,0
plot 'scalog' using (\$2/1000):xtic(1) title "Scalog" fs pattern 7 border rgb "coral" lc rgb 'coral' lw 2,\
 	 'speclog' using (\$2/1000):xtic(1) title "Belfast" fs pattern 3 border rgb "#009988" lc rgb '#009988' lw 2,\
 	 'speclog' using (\$3+0.25):(\$2/1000+0.75):4 title "" with labels center font "Times-new-roman,18"
 	
EOF
) | gnuplot -persist
"""

subprocess.run(['bash'], input=gnuplot, text=True)
subprocess.run(["bash"], input="epstopdf apps.eps", text=True)
subprocess.run(["bash"], input="rm apps.eps scalog speclog", text=True)
subprocess.run(["bash"], input="mv apps.pdf 15.pdf", text=True)
