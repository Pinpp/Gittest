#!/bin/bash
year=$1
date=$2
#count=`dirname data/Y2019/2019-01-2*/*h*`
#count=`ls data/Y2019/2019-01-2*/*h*`
#echo ${count}
#for i in ${count}
#do
#  echo $i
#done
dirname data/Y$year/$date/han*/* > l
count=`sort -u l`
#| uniq l`
echo $count
for j in $count
do
  echo $j
  cp -r $j/../bias $j
  cp -r $j/../flat $j
done
