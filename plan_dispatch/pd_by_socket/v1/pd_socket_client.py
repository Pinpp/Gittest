#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time, json, socket

def load_params(json_file):
    with open(json_file) as read_file:
        params = json.load(read_file)
    return params

def get_host_ip():
    """
    查询本机ip地址
    :return: ip
    """
    ip = 0
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

def pd_socket_client(host,port,cmd):
    beg_mak,end_mak = load_params('./pd_params.json')['socket_mark'][:]
    if cmd:
        s = None
        try:
            s = socket.socket()
            s.connect((host, int(port)))
            s.send(cmd.encode('utf-8'))
            len_date = s.recv(beg_mak)
            #print len_date,len(len_date)
            if int(len_date) == len(end_mak):
                s.close()
                return ''
            count = 1024
            while True:
                a = int(len_date) % count
                if a:
                    break
                count += 1
            data = ''
            while True:
                data_cont = s.recv(count)
                data += data_cont
                if (len(data_cont) < count) and (end_mak in data_cont.decode('utf-8')):
                    data = data[:-len(end_mak)]
                    break
            s.close()
            return ((data.decode('utf-8')).strip()).split('\n')
        except Exception as e:
            print '\nWRONG: %s\n' %e
            if s:
                s.close()
            return 0
    else:
        print '\n\nWARNING: [SOCKET] The cmd is null.\n\n'
        return 0

def pd_socket_client_more(host,port):
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    beg_mak,end_mak = load_params('./pd_params.json')['socket_mark'][:]
    s = socket.socket()
    s.connect((host, int(port)))
    while True:
        cmd = raw_input(">>> ").strip()
        if not cmd:
            continue
        if cmd == 'Q':
            break
        try:
            s.send(cmd.encode('utf-8'))
            len_date = s.recv(beg_mak)
            print int(len_date),len(len_date)
            time.sleep(0.5)
            # res = ''
            # recv_size = 0
            # while recv_size < cmd_res_size:#只要接收的比总大小小，就继续接收
            #     data = sk.recv(1024)
            #     recv_size += len(data)#将本次接收到的大小加到已接收里
            #     res += data#拼接接收的内容
            # else:
            #     print res
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
                data += data_cont
                if (len(data_cont) < count) and (end_mak in data_cont.decode('utf-8','ignore')):
                    data = data[:-len(end_mak)]
                    break
            print data.decode('utf-8','ignore')
        except Exception as e:
            print '\nWrong: %s\n' %e
            break
    s.close()

if __name__ == "__main__":
    host = '190.168.1.207'#get_host_ip()
    port = '33369'
    pd_socket_client_more(host,port)
    # cmd = 'ls /tmp/gfts*.log | sort -r | head -1'
    # cmd = 'ls aaa'
    # cmd = '你好'
    # res = pd_socket_client(host,port,cmd)
    # print res,res[0]
    # #res = res.strip().split('\n')
    # print res,len(res)
    # k = 0
    # for i in res:
    #     k += 1
    #     print i,k
    # beg_mak = 10
    # lens = 333
    # length = format(lens,'%dd'%beg_mak)
    # print length,len(length),len(length.encode('utf-8'))
