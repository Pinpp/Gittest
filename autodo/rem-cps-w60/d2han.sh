#!/bin/bash
year=$1
month=$2
day=$3
#month=$2
#dirname ~/data/Y$year/$year-$month-*/*_*_0* > ll
#count=`sort -u ll`
count=`dirname ~/data/Y$year/$year-$month-$day/*-*-*0* | uniq`
#echo $count
for j in $count
do
  echo $j
  cd $j
  mkdir han
  mv *-*-0* han/
  mv *-*-1* han/
done
