n=$1
for i in $(seq 1 $n)
do
    echo "Running $i-th time"
    ./run.sh 6
    mkdir -p ../../applications/vanilla_applications/intrusion_detection/analytics/intrusion_detection_run_$i
    sudo cp -r ../../applications/vanilla_applications/intrusion_detection/data ../../applications/vanilla_applications/intrusion_detection/analytics/intrusion_detection_run_$i
done
