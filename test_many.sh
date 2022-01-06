#!/bin/bash
###
 # @Author: your name
 # @Date: 2022-01-05 22:39:14
 # @LastEditTime: 2022-01-05 23:15:12
 # @LastEditors: Please set LastEditors
 # @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 # @FilePath: /PCTP-tinykv/test_many.sh
### 
projectName=$1
times=$2
fail=0
prefix="project"
kernel_name="$(uname -s)"
if [ ! $times ]; then
    echo "Times cannot be null"
fi
for ((i=1;i<=${times};i++));
do
make ${prefix}"${projectName}" > test_${projectName}_${i}.log
cat test_${projectName}_${i}.log | grep "FAIL"
if [ $? -ne 0 ];then
    rm test_${projectName}_${i}.log
    if [ $kernel_name = "Darwin" ];then
      rm -rf /var/folders/6n/ck1_j9h96j73hkklj4yv31wc0000gn/*
    else
      rm -r /tmp/test-raftstore*
    fi
    echo "$(date +%Y-%m-%d\ %H:%M:%S) PASS ${i}/${times}"
    sleep 1
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
