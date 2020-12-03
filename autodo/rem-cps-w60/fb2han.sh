#!/bin/bash
year=$1
month=$2
day=$3

#count=`dirname data/Y2019/2019-01-2*/*h*`
#count=`ls data/Y2019/2019-01-2*/*h*`
#echo ${count}
#for i in ${count}
#do
#  echo $i
#done

#dirname ~/data/Y$year/$year-$month-$day/han*/* > l
#count=`sort -u l`

count=`dirname ~/data/Y$year/$year-$month-$day/han*/* | uniq`
#echo $count

for j in $count
do
  echo $j
  cp -r $j/../bias/* $j
  cp -r $j/../flat/*Lum* $j
  #cp -r $j/../flat/*R* $j
done
