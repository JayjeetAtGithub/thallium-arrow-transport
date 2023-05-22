#!/bin/bash
set -ex

node=$1

sudo modprobe ib_uverbs
sudo modprobe ib_ipoib

if [ "$node" == "client" ]; then
    sudo ifconfig ibp130s0 10.0.1.50
else
    sudo ifconfig ibp130s0 10.0.2.50
fi
