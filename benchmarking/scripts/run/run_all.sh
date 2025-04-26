#!/bin/bash

# Runs all the evaluation experiments for the paper
# Usage: ./run_all.sh

echo "Running all experiments..."   

echo "Running RQ1 and RQ2..."
pushd rq1\&2
./run2shard.sh
./run4shard.sh
popd

echo "Running RQ3..."
pushd rq3
./rq3.sh
popd

echo "Running RQ4..."
pushd rq4
./rq4.sh
popd

echo "Running RQ5..."
pushd rq5
./rq5.sh
popd

echo "Running RQ6..."
pushd rq6
./rq6.sh
popd

echo "Running RQ7..."
pushd rq7
./rq7.sh
popd

echo "Running RQ8..."
pushd rq8
./rq8.sh
popd

# TODO RQ9

echo "Running RQ10..."
pushd rq10
./rq10.sh
popd

echo "Done!"