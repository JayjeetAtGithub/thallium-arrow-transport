#!/bin/bash
set -e

selectivity=$1

$PWD/bin/fs $selectivity dataset+mem
