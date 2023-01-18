#!/bin/bash

nohup go run --race mrworker.go indexer.so > ./worker-sh-0 2>&1 & 
nohup go run --race mrworker.go indexer.so > ./worker-sh-1 2>&1 &
nohup go run --race mrworker.go indexer.so > ./worker-sh-2 2>&1 &
nohup go run --race mrworker.go indexer.so > ./worker-sh-3 2>&1 &
nohup go run --race mrworker.go indexer.so > ./worker-sh-4 2>&1 &
nohup go run --race mrworker.go indexer.so > ./worker-sh-5 2>&1 &
nohup go run --race mrworker.go indexer.so > ./worker-sh-6 2>&1 &
nohup go run --race mrworker.go indexer.so > ./worker-sh-7 2>&1 &
