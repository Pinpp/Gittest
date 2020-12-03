#!/usr/bin/expect --

set src [lindex $argv 0]
set dest [lindex $argv 1]

set timeout 30
log_user 0
#spawn -noecho

spawn spawn -noecho bash -c "rsync -rtzp --exclude history $src $dest"
expect {
	"*password*" { send "123456\r" }	
}
expect {
    eof { exit }
}