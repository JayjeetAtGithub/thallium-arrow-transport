#!/bin/bash
set -ex

pwd=$PWD

if [ ! -d "/tmp/duckdb" ]; then
    git clone https://github.com/duckdb/duckdb /tmp/duckdb
fi

cd /tmp/duckdb

git checkout v0.8.0
git pull origin v0.8.0

mkdir -p build/
cd build/

cmake ..
sudo make -j$(nproc) install
sudo cp /usr/local/lib/libduck* /usr/lib

cd $pwd
