#!/bin/bash

src=/home/ccduser/data/
dest=gwac@190.168.1.15:/data/F30_data_proc

while :
do
    ./sync_re_sh.sh $src $dest
    sleep 10
done