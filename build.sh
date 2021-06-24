#!/bin/sh

cd "$(realpath "$(dirname "$0")")"
docker build -t kcri-qaap . | tee build.log
