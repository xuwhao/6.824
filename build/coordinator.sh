#!/bin/bash

export VERBOSE=$1
export TMPDIR=/home/xuwenhao/src/6.824/build/tmp/
export OUTDIR=/home/xuwenhao/src/6.824/src/main/

go build -race -o /home/xuwenhao/src/6.824/build/bin/coordinator /home/xuwenhao/src/6.824/src/main/mrcoordinator.go

echo "build coordinator successfully!"

nohup /home/xuwenhao/src/6.824/build/bin/coordinator /home/xuwenhao/src/6.824/src/main/pg-*.txt > /home/xuwenhao/src/6.824/build/logs/coordinator.log 2>&1 &

tail -200f /home/xuwenhao/src/6.824/build/logs/coordinator.log