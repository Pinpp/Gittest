#!/bin/bash
year=$1
date=$2
#month=$2
#dirname ~/data/Y$year/$year-$month-*/*_*_0* > ll
#count=`sort -u ll`
count=`dirname ~/data/Y$year/$date/*_*_0* | uniq`
echo $count
for j in $count
do
  echo $j
  cd $j
  mkdir han
  mv *_*_0* han/
  mv *_*_-0* han/
done
