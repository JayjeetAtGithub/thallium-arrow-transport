#!/bin/bash
set -ex

sudo mkdir -p /mnt/dataset
for i in {1..200}; do
  sudo cp nyc.parquet /mnt/dataset/nyc.parquet.$i
done
