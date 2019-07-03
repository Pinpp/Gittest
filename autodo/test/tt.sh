#!/usr/bin/expect

set timeout -1

set year [lindex $argv 0]
set date [lindex $argv 1]
set data "~/data/Y$year/$date/han*/*"

spawn bash -c "scp -r $data gwac@172.28.2.11:/data1/sn_survey_w60_old/$date/"

expect "*password*" {send "gwac1234\r"}

expect eof

exit
