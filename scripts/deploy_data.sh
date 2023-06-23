#!/bin/bash
set -ex

sudo mkdir -p /mnt/dataset
sudo mount -t tmpfs -o size=60G tmpfs /mnt/dataset
wget https://skyhook-ucsc.s3.us-west-1.amazonaws.com/16MB.uncompressed.parquet
for i in {1..200}; do
  sudo cp 16MB.uncompressed.parquet /mnt/dataset/16MB.uncompressed.parquet.$i
done
