n=$1
for i in $(seq 1 $n)
do
    echo "Running $i-th time"
    ./run.sh 7
    mkdir -p ../results/apps/transaction_analysis/speclog_$i
    sudo cp -r ../../applications/vanilla_applications/transaction_analysis/data ../results/apps/transaction_analysis/speclog_$i
done
