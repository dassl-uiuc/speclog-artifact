import subprocess
import os 

results_dir = os.getenv("results_dir")

subprocess.run(["python3", "analyze_id.py", results_dir + "/apps/intrusion_detection/"], check=True)
subprocess.run(["python3", "analyze_ta.py", results_dir + "/apps/transaction_analysis/"], check=True)
subprocess.run(["python3", "analyze_hft.py", results_dir + "/apps/hft/"], check=True)


gnuplot = rf"""(cat <<EOF
	set terminal postscript eps enhanced color size 2.3,1.5 font "Times-new-roman,17"
	set output "splitup.eps"
	#set xlabel "Value size (bytes)" font "Times-new-roman,16" offset 0,0.5,0
	set ylabel "E2E latency (ms)" font "Times-new-roman,16" offset 1.5,0,0
	set tmargin 2.5
	set lmargin 5
	set rmargin .6
	set bmargin 3.6
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
	set key vertical at 2.5,12 center samplen 0.8 font "Times-new-roman,15" autotitle columnheader maxrows 2 width -3
	set border 3
	set tics nomirror
	set label 'S: Scalog' at  -0.6,8.45 font "Times-new-roman,16"
	set label 'B: Belfast' at  -0.6,6.85 font "Times-new-roman,16"
	set label "Intrusion\nDetect" at 	-0.5,-3 font "Times-new-roman,16" enhanced
	set label "Fraud\nMonitor" at  2.7,-3 font "Times-new-roman,16" enhanced
	set label "High-Freq\nTrade" at  5.4,-3 font "Times-new-roman,16"  enhanced
    plot "splitup" using (\$3/1000):xtic(1) lt -1 fs solid 0.5, \
	    '' using (\$4/1000) lt -1 fs pattern 7, \
	    '' using (\$5/1000) lt -1 fs solid 0 ,\
	    '' using (\$6/1000) lt -1 fs solid 1 , \
	    '' using (\$9+0.2):(\$7/1000+0.9):8 title "" with labels center font "Times-new-roman,14"

	    
EOF
) | gnuplot -persist
"""

subprocess.run(['bash'], input=gnuplot, text=True)
subprocess.run(["bash"], input="epstopdf splitup.eps", text=True)
subprocess.run(["bash"], input="rm splitup.eps scalog speclog splitup", text=True)
subprocess.run(["bash"], input="mv splitup.pdf 16.pdf", text=True)
