#!/bin/bash

# mount /dev/sda4 and create fs
# sudo mkfs.ext4 /dev/sda4
# if sudo grep -qs "/dev/sda4" /proc/mounts; then
#     sudo umount /dev/sda4
# fi
# sudo mount /dev/sda4 /users/luoxh/scalog-storage
sudo chown -R luoxh /data
sudo rm -rf /data/*