#!/bin/bash
mkdir -p out
javac src/*.java -d out
if [ $? -ne 0 ]; then exit 1; fi

java -cp out TestDBConfig
java -cp out TestDiskManager
java -cp out TestBufferManager
