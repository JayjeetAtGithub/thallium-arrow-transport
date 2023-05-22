#!/bin/bash
set -e

mode=$1

function clean_client_cache {
    sync
    echo 3 > /proc/sys/vm/drop_caches
    sync
}

function clean_server_cache {
    ssh node1 "sync"
    ssh node1 "echo 3 > /proc/sys/vm/drop_caches"
    ssh node1 "sync"
}

query=$(cat /tmp/query)

for i in {1..5}; do
    clean_client_cache
    clean_server_cache
    $PWD/bin/fc "/mnt/cephfs/dataset/*" "$query" "$mode" || true
done
