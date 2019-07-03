#!/bin/bash

year=$1
month=$2
day=$3

./d2han.sh $year $month $day
./fb2han.sh $year $month $day
./rem_cps.sh $year $month $day