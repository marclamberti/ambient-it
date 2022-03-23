#!/bin/bash

# Install Docker
apt-get update

apt-get -y install docker.io

snap install docker

usermod -aG docker $USER

# Install VSCode
apt-get -y install wget gpg
wget -qO- https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > packages.microsoft.gpg
install -o root -g root -m 644 packages.microsoft.gpdockerg /etc/apt/trusted.gpg.d/
sh -c 'echo "deb [arch=amd64,arm64,armhf signed-by=/etc/apt/trusted.gpg.d/packages.microsoft.gpg] https://packages.microsoft.com/repos/code stable main" > /etc/apt/sources.list.d/vscode.list'
rm -f packages.microsoft.gpg

apt -y install apt-transport-https
apt update
apt -y install code # or code-insiders

# Run Airflow
cd ambient-it
docker-compose up -d