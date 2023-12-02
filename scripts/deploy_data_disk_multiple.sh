#!/bin/bash
set -ex

sudo mkdir -p /mnt/dataset

# write the files couple number of times
for i in {1..105}; do
    sudo cp nyc.parquet /mnt/dataset/nyc.$i.parquet
done
