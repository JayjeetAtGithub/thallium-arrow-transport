#!/bin/bash
set -ex

sync
echo 3 > /proc/sys/vm/drop_caches
sync
