n=$1
for i in $(seq 1 $n)
do
    echo "Running $i-th time"
    ./run.sh 1
    mkdir -p runs_$n/$i
    mv ../results/* runs_$n/$i
done
