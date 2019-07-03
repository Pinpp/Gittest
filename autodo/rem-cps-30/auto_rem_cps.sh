#!/bin/bash
#year=$1
date=$1

./d2han.sh $date
./fb2han.sh $date
./rem_cps.sh $date