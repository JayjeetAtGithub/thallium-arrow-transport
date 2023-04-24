#!/bin/bash
set -ex

. ~/spack/share/spack/setup-env.sh
spack load mochi-thallium

cmake .
make -j32
