#!/bin/bash
set -ex

sudo mkdir -p /mnt/dataset

# write the files couple number of times
# 5.88 GB total dataset size
# 50.4 GB total table size Apache Arrow format
for i in {1..105}; do
    sudo cp nyc.parquet /mnt/dataset/nyc.$i.parquet
done
