#!/bin/bash

src=/home/ccduser/data/
dest=/home/data_proc/

while :
do
    rsync -az --exclude history $src $dest
    sleep 10
done