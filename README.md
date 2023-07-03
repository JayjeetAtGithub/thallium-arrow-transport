# thallium-arrow-transport

[![Benchmark](https://github.com/JayjeetAtGithub/thallium-arrow-transport/actions/workflows/benchmark.yml/badge.svg)](https://github.com/JayjeetAtGithub/thallium-arrow-transport/actions/workflows/benchmark.yml)

## Setup Instructions

```bash
git clone https://github.com/JayjeetAtGithub/thallium-arrow-transport
cd thallium-arrow-transport/
./scripts/prep_env.sh
./scripts/config_network.sh [client]/[server]
./scripts/build.sh
./scripts/deploy_data_tmpfs.sh
```

## Running Instructions

```bash
sudo ./scripts/server.sh [ts]/[fs]
sudo ./scripts/client.sh [tc]/[fc]
```
