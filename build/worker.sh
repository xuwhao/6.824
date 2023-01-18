#!/bin/bash

export VERBOSE=$1 

go build -race -buildmode=plugin -o /home/xuwenhao/src/6.824/build/bin/indexer.so /home/xuwenhao/src/6.824/src/mrapps/indexer.go  

echo "build plugin successfully!"

go build -race -o /home/xuwenhao/src/6.824/build/bin/worker /home/xuwenhao/src/6.824/src/main/mrworker.go

echo "build worker successfully!"


for ((i=1;i<=$2;i++));do
	nohup /home/xuwenhao/src/6.824/build/bin/worker /home/xuwenhao/src/6.824/build/bin/indexer.so > /home/xuwenhao/src/6.824/build/logs/worker-{$i}.log 2>&1 &
done
