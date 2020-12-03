#!/bin/bash

obj="screen_monitor.sh"
script="/home/gwac/autostart/screen_monitor.sh"

#gnome-terminal &

while true
do
        n1=`ps -ef|grep $obj|grep -v grep|wc -l`
        if [ $n1 -gt 1 ]; then
                PIDS=`ps -ef|grep $obj|grep -v grep|grep -v PPID|awk '{print $2}'`
                for i in $PIDS
                do
                        kill -9 $i &
                done
                sh $script &
        fi
        n2=`ps -ef|grep $obj|grep -v grep|wc -l`
        if [ $n2 -eq 0 ]; then
                sh $script &
        fi
        sleep 3
done

