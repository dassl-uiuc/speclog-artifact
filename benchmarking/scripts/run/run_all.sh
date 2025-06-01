#!/bin/bash

# Runs all the evaluation experiments for the paper
# Usage: ./run_all.sh

echo "Running all experiments..."   

echo "Running RQ11..."
pushd rq11
./app_failure.sh
./app_failure_mask.sh
popd

echo "Done!"