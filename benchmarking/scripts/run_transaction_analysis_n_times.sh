n=$1
for i in $(seq 1 $n)
do
    echo "Running $i-th time"
    ./run.sh 7
    mkdir -p ../../applications/vanilla_applications/transaction_analysis/analytics/transaction_analysis_run_$i
    sudo cp -r ../../applications/vanilla_applications/transaction_analysis/data ../../applications/vanilla_applications/transaction_analysis/analytics/transaction_analysis_run_$i
done