#!/bin/bash

name="xot"
obj="screen_monitor_monitor.sh"
script="/home/gwac/autostart/screen_monitor_monitor.sh"

n1=`ps -ef|grep $obj|grep -v grep|wc -l`
if [ $n1 -gt 1 ]; then
	PIDS=`ps -ef|grep $obj|grep -v grep|grep -v PPID|awk '{print $2}'`
    for i in $PIDS
    do
        kill -9 $i &
    done
	sh $script &
fi

while :
do 
	n2=`screen -ls|grep $name|grep -v grep|wc -l`
	if [ $n2 -eq 0 ]; then
		screen -dmS $name 
	fi

	n3=`ps -ef|grep $obj|grep -v grep|wc -l`
	if [ $n3 -eq 0 ]; then
		sh $script &
	fi
	sleep 1.5
 done
