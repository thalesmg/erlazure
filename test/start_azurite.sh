#!/usr/bin/env bash

set -exuo pipefail

docker run -d --rm -p 10000:10000 --name azurite mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0 -d debug.log
