#!/bin/bash
set -e

binary=$1
optimize=$2

$PWD/bin/"$binary" "$optimize"
