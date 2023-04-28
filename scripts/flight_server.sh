#!/bin/bash
set -e

selectivity=$1
binary=$2

$PWD/bin/$binary $selectivity dataset+mem