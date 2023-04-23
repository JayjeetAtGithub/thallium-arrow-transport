#!/bin/bash
set -e

selectivity=$1
binary=$2

export PROJECT_ROOT=$HOME/thallium-arrow-transport
$PROJECT_ROOT/bin/$binary $selectivity dataset+mem