#!/bin/bash
set -e

binary=$1
export MARGO_ENABLE_MONITORING=1

$PWD/bin/"$binary"
