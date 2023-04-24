#!/bin/bash
set -ex

sudo modprobe ib_uverbs
sudo modprobe ib_ipoib

sudo ifconfig ibp130s0 10.0.2.50
