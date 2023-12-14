#!/bin/bash
set -e

binary=$1

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

if [ "$binary" == "tc" ]; then
    echo "" > /proj/schedock-PG0/thallium_results
else
    echo "" > /proj/schedock-PG0/flight_results
fi

while IFS= read -r query; do
    for i in {1..5}; do
        if [ "$binary" == "tc" ]; then            
            uri=$(cat /proj/schedock-PG0/thallium_uri)
            $PWD/bin/"$binary" $uri "/mnt/dataset/nyc.*.parquet" "$query" || true
        elif [ "$binary" == "c" ]; then
            uri=$(cat /proj/schedock-PG0/thallium_uri)
            $PWD/bin/"$binary" $uri || true
        elif [ "$binary" == "c2" ]; then
            uri=$(cat /proj/schedock-PG0/thallium_uri)
            $PWD/bin/"$binary" $uri || true
        else
            $PWD/bin/"$binary" "/mnt/dataset/nyc.*.parquet" "$query" || true
        fi
    done
done < queries.txt
