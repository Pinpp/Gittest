#!/usr/bin/expect --

set timeout -1

set year [lindex $argv 0]
set date [lindex $argv 1]
set data "~/data/Y$year/$date/SVOMMM/han*/*"
set rem_path "/media/data12/users/hxhdata/sn_survey/xl30"

spawn ssh -t hxh@herculesii.astro.berkeley.edu "mkdir $rem_path/$date"

expect "*password*" {send "steven\r"}

expect eof

spawn bash -c "scp -r $data hxh@herculesii.astro.berkeley.edu:$rem_path/$date"

expect "*password*" {send "steven\r"}

expect eof

exit
