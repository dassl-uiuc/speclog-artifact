# SpecLog

This repository is the artifact for the OSDI'25 paper titled 
```
Low End-to-End Latency atop a Speculative Shared Log with Fix-Ante Ordering
Shreesha G. Bhat, Tony Hong, Xuhao Luo, Jiyu Hu, Aishwarya Ganesan, Ramnatthan Alagappan
```
This repository builds on the publicly available artifact for [scalog](https://github.com/scalog/scalog)
## Setup
Belfast is a distributed system that contains multiple components, namely a sequencing layer, storage shards, clients and a discovery service. For a minimal setup a cluster with 6 nodes suffices (3 (sequencer nodes) + 2 (two storage shards with discovery service) + 1 (client)). However, for full reproduction of all the experiments in a paper, a cluster with 16 nodes is required. For ease of setup, we request that the source code and binaries be hosted on a network file-system such as NFS/AFS so that all the nodes in this system can share them. From this point on, we assume that the repository is cloned on a shared file-system mounted at `/sharedfs`, all paths in this readme are relative to the root of the repository at `/sharedfs/speclog-artifact`. We also assume that each node (`node0` to `node15`) contains a local disk mounted at `/data`. Additionally, we assume that ssh connections can be established from any node to the others. 


### Environment Variables
Create a `.env` file at `benchmarking/scripts` based on the template in `benchmarking/scripts/.env.example`. 
> **NOTE:** Before running any script below, ensure that the environment variables are loaded. If not, do so by running:  
> ```bash
> source /sharedfs/speclog-artifact/benchmarking/scripts/.env
> ```

### Installation
Run the following steps to install all the required dependencies on all the nodes
```bash
ssh node0
cd /sharedfs/speclog-artifact/benchmarking/scripts
./run_script_on_all.sh ./install_go.sh

# restart bash and reload the environment 
exec bash 
source /sharedfs/speclog-artifact/benchmarking/scripts/.env
```

`node0` will be used to generate the plots from the paper for which we require some additional python libraries which can be installed as follows
```bash
ssh node0
cd /sharedfs/speclog-artifact/benchmarking/scripts
pip install -r requirements.txt

# create a logs directory
cd ..
mkdir logs
```

To compile the binary, run the following 
```bash
ssh node0
cd /sharedfs/speclog-artifact
go build
go mod vendor
```


### Running experiments
All scripts are available in the `benchmarking/scripts` sub-directory. Some important ones are listed below
1. `common.sh` contains various functions to setup the ordering layer, data shards, start the clients and collect the results
2. `run_script_*` is a helper to run any local script on a set of specific nodes simultaneously
3. `run_*.sh` scripts run specific experiments from the paper 
4. `analyze*` contains python scripts/jupyter notebooks to analyze experimental results

To run a simple experiment to check whether the setup was successful, run the following command

```bash
cd /sharedfs/speclog-artifact/benchmarking/scripts
./run_e2e.sh 
```

This script runs a simple e2e benchmark for 1 minute by first setting up a Belfast cluster with 2 storage shards and 3 sequencing layer nodes. Two client nodes are subsequently spawned. Each client node hosts 10 append clients and 1 consumer connected to one storage shard. Each append client appends 4k records at a rate of 1000/sec and each consumer runs a fake compute operation of 1ms/op. Once the experiment terminates, the result files are accumulated in `/sharedfs/speclog-artifact/benchmarking/results/e2e_1000`. These results can be analyzed through the following command

```bash
cd /sharedfs/speclog-artifact/benchmarking/scripts
python3 analyze_e2e.py
```

The result of the above should look something like this
```
results for computation time 1000 us
statistic/metric, latency (us)
mean, 2950.13
p50, 2823.00
p99, 5062.00
total throughput, 19927.97
statistic/metric, delivery latency (us), compute latency (us), confirm latency (us), e2e latency (us), queuing delay (us)
mean, 983.82, 2495.56, 2963.45, 3032.32, 506.96
std, 426.34, 515.84, 756.11, 726.23, 290.88
p50, 947.00, 2482.00, 2841.00, 2919.00, 508.00
p99, 1864.00, 3539.00, 5111.00, 5132.00, 999.00
```

### Reproducing the results 
Using the base scripts from earlier, we have constructed individual scripts for each research question in the evaluation section of the paper. These scripts are available in 
`benchmarking/scripts/run/rq*`. For our baseline, we have Scalog which is hosted in a separate branch `scalog-impl`. This branch also contains similar scripts to reproduce results from the paper at the same path. Additionally, for RQ9, our code is hosted on a separate branch `failure-expt`.  

To run all the experiments from the paper, run the following sequence of commands
```bash
cd /sharedfs/speclog-artifact
git checkout main

# build the belfast binary 
go build
go mod vendor


cd benchmarking/scripts/run
# run all experiments, this step should take about 105-110 mins
./run_all.sh 

# go back to root and checkout to the failure experiment branch (for RQ9)
cd ../../..
git checkout failure-expt

# build this version
go build
go mod vendor

# run RQ9 (should take about a minute)
cd benchmarking/scripts/run
./run_all.sh 

# go back to root and checkout to scalog
cd ../../..
git checkout scalog-impl

# build scalog 
go build
go mod vendor

cd benchmarking/scripts/run
# run all experiments for scalog, this step should take about 80-85 mins
./run_all.sh 
```

These steps generate all the result files under `$results_dir`. To analyze the results and generate plots, run the following

```bash
cd /sharedfs/speclog-artifact
# if not already on main, checkout to main
git checkout main 

cd benchmarking/scripts/plot
mkdir figs

# plot all the results and save the pdfs to figs
./plot_all.sh figs/
```


### For artifact evaluators

For artifact evaluators, we provide a cluster with 16 nodes. These nodes are already setup with the installation and setup steps. In these nodes, the repository would be hosted in an NFS at `/proj/rasl-PG0/<username>/speclog-artifact`. Please talk to us through the preferred communication medium for further instructions to access this cluster. 

If any step or script gets stuck at any point, exit from the script and perform the following steps to cleanup any stale processes on the servers and clients
```
cd /sharedfs/speclog-artifact/benchmarking/script
./run.sh 5
```

> **NOTE:** README from the original artifact for scalog below

# scalog
reimplementing scalog from scratch

## Build Scalog

Run the `go build` command to build Scalog, and `go test -v ./...` to run
unit-tests.

## Run the code

To run the server side code, we have to install `goreman` by running
```go
go get github.com/mattn/goreman
```

Use `goreman start` to run the server side code. The goreman configuration is
in `Procfile` and the Scalog configuration file is in `.scalog.yaml`

To run the client, we should use
```go
./scalog client --config .scalog.yaml
```

After that, we can use command `append [record]` to append a record to the
log, and use `read [GlobalSequenceNumber] [ShardID]` to read a record.
