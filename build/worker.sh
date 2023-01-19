#!/bin/bash

export VERBOSE=$1 
export TMPDIR=/home/xuwenhao/src/6.824/build/tmp/
export OUTDIR=/home/xuwenhao/src/6.824/src/main/

go build -race -buildmode=plugin -o /home/xuwenhao/src/6.824/build/bin/plugin.so $3

echo "build plugin successfully!"

go build -race -o /home/xuwenhao/src/6.824/build/bin/worker /home/xuwenhao/src/6.824/src/main/mrworker.go

echo "build worker successfully!"


for ((i=1;i<=$2;i++));do
	nohup /home/xuwenhao/src/6.824/build/bin/worker /home/xuwenhao/src/6.824/build/bin/plugin.so > /home/xuwenhao/src/6.824/build/logs/worker-$i.log 2>&1 &
done
