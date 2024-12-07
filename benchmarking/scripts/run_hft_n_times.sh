n=$1
for i in $(seq 1 $n)
do
    echo "Running $i-th time"
    ./run.sh 8
    mkdir -p ../../applications/vanilla_applications/hft/analytics/hft_run_$i
    sudo cp -r ../../applications/vanilla_applications/hft/data ../../applications/vanilla_applications/hft/analytics/hft_run_$i
done
