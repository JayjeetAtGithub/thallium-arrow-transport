#!/bin/bash
set -e

binary=$1

export PROJECT_ROOT=$PWD

uri=$(ssh node1 "cat /tmp/thallium_uri")
$PROJECT_ROOT/bin/$binary $uri