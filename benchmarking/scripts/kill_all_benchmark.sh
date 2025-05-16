#! /bin/bash

sudo pkill -f "append_bench"
sudo pkill -f "sequential_read_bench"
sudo pkill -f "single_client_e2e"
sudo pkill -f "reconfig"
sudo pkill -f "lagfix"
sudo pkill -f "transaction_analysis_failure"
sudo pkill -f "single_client_e2e_3way"