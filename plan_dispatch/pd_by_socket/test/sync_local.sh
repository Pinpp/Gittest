#!/bin/bash

src=/home/ccduser/data/
dest=/home/data_proc/

inotifywait -mr --timefmt '%Y/%d/%m %H:%M:%S' --format '%T %e %w %f' -e create,delete,modify $src | while read text
do
    echo -e "\n\n$text\n"
    rsync -azv --exclude history $src $dest
    date=`date`
    echo -e "\n$date\n\n"
done