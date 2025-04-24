#! /bin/bash

sudo pkill -f "append_bench"
sudo pkill -f "sequential_read_bench"
sudo pkill -f "single_client_e2e"
sudo pkill -f "reconfig"
sudo pkill -f "lagfix"
sudo pkill -f "quota_change"
sudo pkill -f "intrusion_detection_generator"
sudo pkill -f "intrusion_detection_devices"
sudo pkill -f "transaction_analysis"
sudo pkill -f "transaction_analysis_generator"
sudo pkill -f "hft"
sudo pkill -f "hft_generator"
