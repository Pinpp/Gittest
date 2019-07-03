#!/bin/bash
#year=$1
date=$1

year="2019"

#month=$2
#dirname ~/data/Y$year/$year-$month-*/*_*_0* > ll
#count=`sort -u ll`
count=`dirname ~/data/Y$year/$date/SVOMMM/G* | uniq`
echo $count
for i in $count
do
  echo ''
  echo $i
  cd $i
  mkdir han
  cp G*/G*.fit han/
done
