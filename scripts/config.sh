#!/bin/bash
set -ex

modprobe ib_uverbs
modprobe ib_ipoib

ifconfig ibp130s0 10.0.1.50
