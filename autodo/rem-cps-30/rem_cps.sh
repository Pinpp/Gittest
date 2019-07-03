#!/bin/bash

#year=$1
date=$1

year="2019"

dirname ~/data/Y$year/$date/SVOMMM/han*/* > l
count=`sort -u l`
echo $count
for i in $count
do
  echo ''
  echo $i
  j=`dirname $i`
  k=`dirname $j`
  x=`basename $k`
  ./rem_cp.sh $year $x
done
