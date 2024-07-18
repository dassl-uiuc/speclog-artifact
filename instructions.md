## Instructions
Instructions for setting up and running this repository on an existing cloudlab cluster

### Pre-requistes
Unfortunately, in the existing scripts, usernames and paths are hardcoded, so replace all occurances of `sgbhat3` with your cloudlab username. Additionally, it is expected that the repository is cloned as `speclog` within a sub-directory under the user's username in the shared NFS folder, i.e., 
```
mkdir /proj/rasl-PG0/<username>/
cd /proj/rasl-PG0/<username>/
git clone git@github.com:dassl-uiuc/speclog.git
```
It is also expected that the disk used to store local logs as well as data in the shared log, on each machine is mounted at `/data`, kindly verify this by running 
`df` or `lsblk`. Another expectation is that your passless ssh key is stored at `/users/<username>/.ssh/id_rsa`, change this if necessary. 

### Installing Go and running experiments
* Modify the number of servers accordingly in `benchmarking/scripts/run_script_on_all.sh` and run 
```
./run_script_on_all.sh ./install_go.sh
```
* Only build the `scalog` object on one machine (it is shared through NFS) by running `go build` within the `speclog` directory. 
* The main script to run the experiments is at `benchmarking/scripts/run.sh`. For running append only benchmarks, the current setup requires you to run this script as `./run.sh 0`. Other parameters such as the number of data nodes, number of append clients, append data sizes etc must be specified within the `run.sh` script and various sub-scripts it uses. Once the run finishes, it creates a results directory under `benchmarking`. One can analyze the results by changing the number of clients and path of the results directory in `benchmarking/scripts/analyze.py` and then running the script with no arguments. 

### Protobuf Installation for regenerating code
Manually install `protobuf-1.3.2`
```
wget https://github.com/golang/protobuf/archive/refs/tags/v1.3.2.zip
unzip v1.3.2.zip
cd protobuf-1.3.2
make

# install protoc
sudo apt-get install protobuf-compiler
```
Move back to the project directory and execute the following to regenerate protobuf code
```
./genpb.sh
```