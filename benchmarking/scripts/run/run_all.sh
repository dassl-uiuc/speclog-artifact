#!/bin/bash

# Runs all the evaluation experiments for the paper
# Usage: ./run_all.sh

echo "Running all experiments..."   

echo "Running RQ8..."
pushd rq8
./rq8.sh
popd

echo "Done!"