#!/bin/bash
#year=$1
date=$1

year="2019"

#count=`dirname data/Y2019/2019-01-2*/*h*`
#count=`ls data/Y2019/2019-01-2*/*h*`
#echo ${count}
#for i in ${count}
#do
#  echo $i
#done

#dirname ~/data/Y$year/$year-$month-$day/han*/* > l
#count=`sort -u l`

count=`dirname ~/data/Y$year/$date/SVOMMM/han*/* | uniq`
echo $count

for i in $count
do
  echo ''
  echo $i
  cp -r $i/../../bias/* $i
  cp -r $i/../../flat/*R* $i
  cp -r $i/../../dark/* $i
done
