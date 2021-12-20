#!/bin/bash
# Run make project2b many times.
times=0

if test $# -eq 1
then
  times=1
else
  times=$1
fi

echo "${times}"

while (( ${times} >0))
do 
  make project2b
done
