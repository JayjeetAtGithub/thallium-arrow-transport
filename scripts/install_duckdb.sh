#!/bin/bash
set -ex

pwd=$PWD

if [ ! -d "/tmp/duckdb" ]; then
    git clone https://github.com/duckdb/duckdb /tmp/duckdb
fi

cd /tmp/duckdb
git pull

mkdir -p build/
cd build/

cmake ..
make -j$(nproc) install

cd $pwd
