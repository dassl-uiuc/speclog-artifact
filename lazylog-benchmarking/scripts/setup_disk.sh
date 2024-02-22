#!/bin/bash

# mount /dev/sda4 and create fs
# sudo mkfs.ext4 /dev/sda4
# sudo mkdir /scalog-storage
sudo mount /dev/sda4 /scalog-storage
sudo chown -R sgbhat3 /scalog-storage
sudo rm -rf /scalog-storage/*