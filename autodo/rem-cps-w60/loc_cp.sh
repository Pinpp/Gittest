#!/usr/bin/expect

set timeout -1

set year [lindex $argv 0]
set date [lindex $argv 1]
set data "~/data/Y$year/$date/han*/*"
set path "/data/data/hanxuhui_sn_survey/$date"

#spawn bash -c "scp -r $data hxh@herculesii.astro.berkeley.edu:/media/data12/users/hxhdata/sn_survey/$date"

spawn ssh -t root@190.168.1.18 "mkdir $path"

expect "*password*" {send "123456\r"}

expect eof

spawn bash -c "scp -r $data root@190.168.1.18:$path"

expect "*password*" {send "123456\r"}

expect eof

exit
