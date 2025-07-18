#! /bin/bash

cd ~
sudo apt update

ARCH=$(uname -m)

if [ "$ARCH" = "aarch64" ]; then
  GO_TAR="go1.22.0.linux-arm64.tar.gz"
elif [ "$ARCH" = "x86_64" ]; then
  GO_TAR="go1.22.0.linux-amd64.tar.gz"
else
  echo "unsupported architecture: $ARCH"
  exit 1
fi

# install go
wget https://go.dev/dl/$GO_TAR
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf $GO_TAR
sudo rm -rf $GO_TAR
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
echo "export PATH=\$PATH:/users/${username}/go/bin" >> ~/.bashrc
export PATH=$PATH:/usr/local/go/bin

# install goreman
go install github.com/mattn/goreman@latest
sudo cp /users/${username}/go/bin/goreman /usr/local/bin/


# own user
sudo chown -R $username ~

# install packages
sudo apt-get install pip texlive-font-utils gnuplot -y 

# run on any one node
# TODO
# sudo apt-get install msttcorefonts -qq -y

