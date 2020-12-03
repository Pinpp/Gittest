#!/bin/bash

year=$1
date=$2

dirname data/Y$year/$date/han* > l
count=`sort -u l`
echo $count
for j in $count
do
  echo $j
  k=`basename $j`
  ./tt.sh $year $k
done
