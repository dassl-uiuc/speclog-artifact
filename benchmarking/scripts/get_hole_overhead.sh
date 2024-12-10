
dir=("append_with" "append_wo")
cli=(20 40 60 80 100)

for d in ${dir[@]}; do
    for c in ${cli[@]}; do
        htput=$(python3 get_hole_throughput.py 120 append_scalability/$d/3/logs/1ms/append_bench_${c}_1000/)
        echo "$d $c $htput"
    done
done
