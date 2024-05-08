#!/bin/bash
sudo apt-get install cgroup-tools -y
sudo cgcreate -g memory:/memlimited

sudo echo 250M > /sys/fs/cgroup/memory/memlimited/memory.limit_in_bytes