#!/bin/bash

# mount /dev/sda4 and create fs
# sudo mkfs.ext4 /dev/sda4
if sudo grep -qs "/dev/sda4" /proc/mounts; then
    sudo umount /dev/sda4
fi
sudo mount /dev/sda4 /users/sgbhat3/scalog-storage
sudo rm -rf /users/sgbhat3/scalog-storage/*