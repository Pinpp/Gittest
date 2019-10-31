#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time, json, socket
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def load_params(json_file):
    with open(json_file) as read_file:
        params = json.load(read_file)
    return params

def pd_socket_client(host,port,cmd):
    beg_mak,end_mak = load_params('./pd_params.json')['socket_mark'][:]
    if cmd:
        try:
            s = socket.socket()
            s.connect((host, int(port)))
            s.send(cmd.encode('utf-8'))
            len_date = s.recv(beg_mak)
            #print len_date,len(len_date)
            if int(len_date) == len(end_mak):
                s.close()
                return ''
            #time.sleep(0.5)
            #data = s.recv(int(len_date))
            count = 1024
            while True:
                a = int(len_date) % count
                if a:
                    break
                count += 1
            data = ''
            while True:
                data_cont = s.recv(count)
                #print data_cont
                data += data_cont
                if (len(data_cont) < count) and (end_mak in data_cont.decode('utf-8')):
                    data = data[:-len(end_mak)]
                    break
            s.close()
            return ((data.decode('utf-8')).strip()).split('\n')
        except Exception as e:
            print '\n\nWARNING: [SOCKET] %s\n\n' %e
            return 0
    else:
        print '\n\nWARNING: [SOCKET] The cmd is null.\n\n'
        return 0

def pd_socket_client_more(host,port):
    beg_mak,end_mak = load_params('./pd_params.json')['socket_mark'][:]
    s = socket.socket()
    s.connect((host, int(port)))
    while True:
        cmd = raw_input(">>> ").strip()
        if not cmd:
            continue
        try:
            s.send(cmd.encode('utf-8'))
            len_date = s.recv(beg_mak)
            #print len_date,len(len_date)
            if int(len_date) == len(end_mak):
                continue
            count = 1024
            while True:
                a = int(len_date) % count
                if a:
                    break
                count += 1
            data = ''
            while True:
                data_cont = s.recv(count)
                #print data_cont
                data += data_cont
                if (len(data_cont) < count) and (end_mak in data_cont.decode('utf-8')):
                    data = data[:-len(end_mak)]
                    break
            #data = s.recv(int(len_date))
            print data.decode('utf-8')
        except Exception as e:
            print '\n\nWARNING: %s\n\n' %e
            break
    s.close()

if __name__ == "__main__":
    host = '172.28.1.11'
    port = '33369'
    # s = socket.socket()
    # s.connect((host, int(port)))
    # s.send('Welcome.')
    # print s.recv(1024)
    pd_socket_client_more(host,port)
    # cmd = 'ls /tmp/gfts*.log | sort -r | head -1'
    # res = pd_socket_client(host,port,cmd)
    # print type(res)
    # print res
    # #res = res.strip().split('\n')
    # print res,len(res)
    # k = 0
    # for i in res:
    #     k += 1
    #     print i,k
    # cmd = 'cat /tmp/gftservice_20191030_1.log'
    # res = pd_socket_client(host,port,cmd)
    # print type(res)
    # print res,len(res)