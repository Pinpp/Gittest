#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time, socket


def pd_socket_client(host,port,cmd):
    if cmd:
        try:
            s = socket.socket()
            s.connect((host, int(port)))
            s.send(cmd.encode('utf-8'))
            len_date = s.recv(10)
            #print len_date,len(len_date)
            if int(len_date) == 0:
                s.close()
                return ''
            time.sleep(0.5)
            data = s.recv(int(len_date))
            s.close()
            return ((data.decode('utf-8')).strip()).split('\n')
        except Exception as e:
            print '\n\nWARNING: [SOCKET] %s\n\n' %e
            return 0
    else:
        print '\n\nWARNING: [SOCKET] The cmd is null.\n\n'
        return 0

def pd_socket_client_xx(host,port,cmd):
    if cmd:
        try:
            s = socket.socket()
            s.connect((host, int(port)))
            s.send(cmd.encode('utf-8'))
            len_date = s.recv(10)
            #print len_date,len(len_date)
            if int(len_date) == 0:
                #s.close()
                return ''
            time.sleep(0.5)
            data = s.recv(int(len_date))
            s.close()
            return data.decode('utf-8').strip().split('\n')
        except Exception as e:
            print '\n\nWARNING: [SOCKET] %s\n\n' %e
            return 0
    else:
        print '\n\nWARNING: [SOCKET] The cmd is null.\n\n'
        return 0

def pd_socket_client_more(host,port):
    s = socket.socket()
    s.connect((host, int(port)))
    while True:
        cmd = raw_input(">>> ").strip()
        if not cmd:
            continue
        try:
            s.send(cmd.encode('utf-8'))
            len_date = s.recv(10)
            #print len_date,len(len_date)
            if int(len_date) == 0:
                continue
            data = s.recv(int(len_date))
            print data.decode('utf-8')
        except Exception as e:
            print '\n\nWARNING: [SOCKET] %s\n\n' %e

if __name__ == "__main__":
    host = '190.168.1.203'
    port = '33369'
    # s = socket.socket()
    # s.connect((host, int(port)))
    # s.send('Welcome.')
    # print s.recv(1024)
    #pd_socket_client_more(host,port)
    cmd = 'ls /tmp/gfts*.log | sort -r | head -1'
    res = pd_socket_client(host,port,cmd)
    print type(res)
    print res
    #res = res.strip().split('\n')
    print res,len(res)
    k = 0
    for i in res:
        k += 1
        print i,k
    cmd = 'cat /tmp/gftservice_20191030_1.log'
    res = pd_socket_client(host,port,cmd)
    print type(res)
    print res,len(res)