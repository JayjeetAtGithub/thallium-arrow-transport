#!/bin/bash
set -ex

sudo apt update
sudo apt install -y ibverbs-utils pslist

./scripts/install_arrow.sh
./scripts/install_duckdb.sh

rm -rf ~/spack
git clone -b releases/v0.18 -c feature.manyFiles=true https://github.com/spack/spack.git ~/spack
. ~/spack/share/spack/setup-env.sh

rm -rf ~/mochi-spack-packages
git clone https://github.com/mochi-hpc/mochi-spack-packages.git ~/mochi-spack-packages
spack repo add ~/mochi-spack-packages || true

spack install libfabric@1.18.1 fabrics=tcp,udp,sockets,verbs,rxm
spack install --reuse mercury@2.3.0 ~checksum ~boostsys
spack install --reuse mochi-margo@main
spack install --reuse mochi-thallium@0.11.2
