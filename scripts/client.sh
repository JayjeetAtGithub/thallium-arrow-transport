#!/bin/bash
set -e

binary=$1

export PROJECT_ROOT=$PWD

uri=$(cat /proj/schedock-PG0/thallium_uri)
echo "Connecting to $uri"

$PROJECT_ROOT/bin/$binary $uri