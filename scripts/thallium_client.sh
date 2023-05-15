#!/bin/bash
set -e

uri=$(cat /proj/schedock-PG0/thallium_uri)
echo "Connecting to $uri"

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

for i in {1..5}; do
    clean_client_cache
    clean_server_cache
    $PWD/bin/tc $uri "/mnt/cephfs/dataset/*" "SELECT * FROM dataset WHERE total_amount > 69" || true
done
