#!/bin/bash

START=$1
END=$2
REPEAT=$3
TESTS=""
i=0

while [ $START -le $END ]
do
  TESTS="${TESTS}${START} "
  START=$(( $START + 1 ))
done

echo "Running tests [ $TESTS] $REPEAT times..."

while [ $i -lt $REPEAT ]
do
  echo "====> $i-th Round"
  killall lock_server; rm *.log; ./rsm_tester.pl $TESTS
  i=$(( $i + 1 ))
done