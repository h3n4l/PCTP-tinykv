#!/bin/bash
projectName=$1
times=$2
fail=0
prefix="project"
if [ ! $times ]; then
    echo "Times cannot be null"
fi
for ((i=1;i<=${times};i++));
do
make ${prefix}"${projectName}" > test_${projectName}_${i}.log
cat test_${projectName}_${i}.log | grep "FAIL"
if [ $? -ne 0 ];then
    rm test_${projectName}_${i}.log
    rm -r /tmp/test-raftstore*
    echo "$(date +%Y-%m-%d\ %H:%M:%S) PASS ${i}/${times}"
    sleep 3
else
    echo "$(date +%Y-%m-%d\ %H:%M:%S) FAIL THIS TEST"
    fail=1
    break
fi
done
if [ $fail -ne 0 ];then
  echo "[×] : FAIL SOME TESTS"
else
  echo "[√] : PASS ALL TEST"
fi
