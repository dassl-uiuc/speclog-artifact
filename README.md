# SpecLog

This repository is the artifact for the OSDI'25 paper titled 
```
Low End-to-End Latency atop a Speculative Shared Log with Fix-Ante Ordering
Shreesha G. Bhat, Tony Hong, Xuhao Luo, Jiyu Hu, Aishwarya Ganesan, Ramnatthan Alagappan
```
This repository builds on the publicly available artifact for [scalog](https://github.com/scalog/scalog)
## Setup
Belfast is a distributed system that contains multiple components, namely a sequencing layer, storage shards, clients and a discovery service. For a minimal setup a cluster with 6 nodes suffices (3 (sequencer nodes) + 2 (two storage shards with discovery service) + 1 (client)). However, for full reproduction of all the experiments in a paper, a cluster with 16 nodes is required. For ease of setup, we request that the source code and binaries be hosted on a network file-system such as NFS/AFS so that all the nodes in this system can share them. From this point on, we assume that the repository is cloned on a shared file-system mounted at `/sharedfs`, all paths in this readme are relative to the root of the repository at `/sharedfs/speclog-artifact`. We also assume that each node (`node0` to `node15`) contains a local disk mounted at `/data`. Additionally, we assume that ssh connections can be established from any node to the others. 

### Aside: On NFS/AFS usage
A distributed file system such as NFS/AFS is used to host the program binary, configuration and scripts consistently across the servers. Therefore, it only simplifies the development and experimental cycle, and is not fundamental to Belfast. For CloudLab-based setups note that CloudLab already provides NFS-based sharing as part of the [`/proj` heirarchy](https://docs.cloudlab.us/advanced-storage.html#(part._shared-storage)). 


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

`node0` will be used to generate the plots from the paper for which we require some additional python libraries and dependencies which can be installed as follows
```bash
ssh node0
cd /sharedfs/speclog-artifact/benchmarking/scripts
pip install -r requirements.txt

# install fonts
sudo apt-get install msttcorefonts -qq -y # needs manual acknowledgement

# create a logs directory
cd ..
mkdir logs
```

To compile the Belfast binary, run the following 
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
`benchmarking/scripts/run/rq*`. For our baseline, we have Scalog which is hosted in a separate branch `scalog-impl`. This branch also contains similar scripts to reproduce results from the paper at the same paths. For the fault-tolerance experiment, our code is hosted on a separate branch `failure-expt`. Emulation code for the scalability experiment is on `speclog-emulation` and `scalog-emulation`. 

To run experiments related to an individual research question, simply navigate to the required `benchmarking/scripts/run/rq*` directory and run the corresponding scripts. For ease-of-reproduction we have stitched together all the run scripts in a `run_all.sh` script for each branch. The commands below describe how to run the `run_all.sh` scripts from each branch to generate all the experimental data to plot results. 

To run all the experiments from the paper, run the following sequence of commands

* Build Belfast
```bash
cd /sharedfs/speclog-artifact
git checkout main

# build the belfast binary 
go build
go mod vendor
```

* Run all experiments for Belfast
```bash
cd benchmarking/scripts/run
# run all experiments, this step should take about 100-110 mins
./run_all.sh 
```

* Build the failure handling experiment version of Belfast 
```bash
# go back to root and checkout to the failure experiment branch (for RQ9)
cd ../../..
git checkout failure-expt

# build this version
go build
go mod vendor
```

* Run failure experiment on Belfast
```bash
# run RQ9 (should take about a minute)
cd benchmarking/scripts/run
./run_all.sh 
```

* Build Scalog
```bash
# go back to root and checkout to scalog
cd ../../..
git checkout scalog-impl

# build scalog 
go build
go mod vendor
```

* Run all experiments for Scalog
```bash
cd benchmarking/scripts/run
# run all experiments for scalog, this step should take about 105-110 mins
./run_all.sh 
```

* Build emulation for Belfast
```bash 
cd ../../..
git checkout speclog-emulation

# build
go build
go mod vendor
```

* Run all emulation experiments for Belfast
```bash
cd benchmarking/scripts/run
./run_all.sh
```

* Build emulation version of Scalog
```bash
cd ../../..
git checkout scalog-emulation

# build variant
go build
go mod vendor
```

* Run all emulation experiments for Scalog
```bash
cd benchmarking/scripts/run
./run_all.sh
```



These steps generate all the experimental data files under `$results_dir`. To analyze the results and generate plots, run the following

```bash
cd /sharedfs/speclog-artifact
# if not already on main, checkout to main
git checkout main 

cd benchmarking/scripts/plot
mkdir figs

# plot all the results and save the pdfs to figs
./plot_all.sh figs/
```

This step generates all the figures as per the figure numbering in the paper. For example, Figure 6(a) from the paper will appear as `figs/6a.pdf`. 


## Code Organization
This repository is structured as follows
1. The `client/` sub-directory contains client-side code that implements the `Client` struct. This sub-directory implements a part of the client-library responsibilities such as connecting to shard replicas, sending appended records to the corresponding replica, subscribing to records, waiting for acknowledgements/confirmations/mis-speculations. 
2. The `scalog_api/` sub-directory is a wrapper around the `Client` struct to present a further simplified interface to end applications. For example, it filters the no-op records added by Belfast before delivering records to the application. 
3. The `order/` sub-directory implements the sequencing layer logic for Belfast. This includes listening to the shard replicas for join/leave requests, listening to the shard replica reports, assigning quotas, sending actual cuts. Most of the logic is encapsulated within `order/order_server.go`. The `order/orderpb` sub-directory contains the protocol buffer messages and RPCs used for shard-to-sequencer communications. 
4. The `data/` sub-directory implements the shard-side logic for Belfast. This includes sending reports, join/leave requests, delivering records to clients, assigning sequence numbers, sending confirmations/acknowledgements/mis-speculations to clients, storing log records persistently etc. Most of the logic is contained within `data/data_server.go`. Similarly, the `data/datapb` contains the protocol buffer messages and RPCs used for client-to-shard communication. 
5. The `storage/` sub-directory implements the shard-side component for managing log records on disk. 
6. The `discovery/` sub-directory implements the discovery service for clients to learn about view changes (shards joining/leaving/failing). 
7. The `benchmarking/` sub-directory contains scripts for running the experiments as described above. 
8. The `applications/vanilla_applications` implements the code for the applications evaluated in the paper.  


### Building applications atop Belfast 
Applications interact with Belfast through the `Scalog` struct and API in `scalog_api/scalog_api.go`. For a detailed interface and usage, refer to any application under `applications/vanilla_applications`. For example, `applications/vanilla_applications/intrusion_detection/intrusion_detection_generator.go` contains the appender side code for the application and `applications/vanilla_applications/intrusion_detection/intrusion_detection_devices.go` contains the downstream consumer logic.

## For artifact evaluators
For artifact evaluators, we provide a cluster with 16 nodes. These nodes are already setup with the installation and setup steps. In these nodes, the repository would be hosted in an NFS at `/proj/rasl-PG0/<username>/speclog-artifact`. Please talk to us through HotCRP for further instructions to access this cluster. 

If any step or script gets stuck at any point, exit from the script and perform the following steps to cleanup any stale processes on the servers and clients
```bash
cd /sharedfs/speclog-artifact/benchmarking/scripts
./run.sh 5
```

While we have done our best to script the experiments, please note that some of these experiments involve very carefully injected faults, bursts or other abnormal behaviours and consequently the desired system behaviour might not be obtained in each run. If errors are encountered either at runtime or malformed figures are obtained during plotting, kindly rerun the experiments corresponding to the specific research question in the corresponding branches as below. Please contact us through HotCRP if you have any questions. 

```bash 
cd /sharedfs/speclog-artifact/benchmarking/scripts/run/rq<insert_number>
./rq<insert_number>.sh
```

## Supported Platforms
Belfast has been tested on a cluster with the following configuration

* OS: Ubuntu 22.04 LTS
* NIC: Mellanox MT27710 Family ConnectX-4 Lx (25Gb RoCE)
* System: 10-core Intel E5-2640v4 at 2.4 GHz with 64GB ECC Memory (4x 16 GB DDR4-2400 DIMMs)
* Disk: Intel DC S3520 480 GB 6G SATA SSD

The above configuration is available on the `xl170` machines on the CloudLab platform.
 
---
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
