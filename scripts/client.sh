#!/bin/bash
set -e

binary=$1
mode=$2

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

    if [ "$binary" == "tc" ]; then
        uri=$(cat /proj/schedock-PG0/thallium_uri)
        $PWD/bin/"$binary" $uri "/mnt/cephfs/dataset/*" "$query" "$mode" || true
    else
        $PWD/bin/"$binary" "/mnt/cephfs/dataset/*" "$query" "$mode" || true
    fi
done
