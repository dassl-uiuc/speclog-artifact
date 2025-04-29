#!/bin/bash

# Runs all the evaluation experiments for the paper
# Usage: ./run_all.sh

echo "Running all experiments..."   

echo "Running RQ9..."
pushd rq9
./rq9.sh
popd

echo "Done!"