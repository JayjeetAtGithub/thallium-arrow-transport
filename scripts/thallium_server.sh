#!/bin/bash
set -e

selectivity=$1

$PWD/bin/ts $selectivity dataset+mem
