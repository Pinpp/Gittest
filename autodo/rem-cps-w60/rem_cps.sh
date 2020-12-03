#!/bin/bash

year=$1
month=$2
day=$3

dirname ~/data/Y$year/$year-$month-$day/han*/* > l
count=`sort -u l`
echo $count
for i in $count
do
  echo $i
  j=`dirname $i`
  k=`basename $j`
  ./rem_cp.sh $year $k
done
