#! /bin/bash

cd ~
sudo apt update

# install go
wget https://go.dev/dl/go1.22.0.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.22.0.linux-amd64.tar.gz
sudo rm -rf go1.22.0.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
echo 'export PATH=$PATH:/users/sgbhat3/go/bin' >> ~/.bashrc
export PATH=$PATH:/usr/local/go/bin

# install goreman
go install github.com/mattn/goreman@latest
