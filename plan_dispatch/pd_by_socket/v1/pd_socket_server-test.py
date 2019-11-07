#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import json
import socket
import subprocess
import threading
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

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

def get_ser_conf(conf_file):
    socket_confs = load_params(conf_file)['socket']
    ips = {i[0]:i[1] for i in socket_confs.values()}
    ip = get_host_ip()
    if not ip:
        return 0
    if ip in ips:
        port = ips[ip]
        return (ip,int(port))
    else:
        return 0

def pd_socket_server():
    conf_file = './pd_socket_server_params.json'#'./pd_socket_params.json' 
    beg_mak,end_mak = load_params(conf_file)['socket_mark'][:]
    #socket_conf = get_ser_conf(conf_file)
    socket_conf = (get_host_ip(),33369)
    if socket_conf:
        print 'THIS: ', socket_conf
        s = socket.socket()
        s.bind(socket_conf)
        s.listen(5)
        while True:
            c, addr = s.accept()
            print 'Address: ', addr
            while True:
                try:
                    msg = c.recv(1024)
                    print msg
                    if not msg:
                        break
                    cmd = subprocess.Popen(msg.decode('utf-8','ignore'), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    cmdout = cmd.stdout.read()
                    cmderr = cmd.stderr.read()
                    lens = len(cmdout.encode('utf-8')) + len(cmderr.encode('utf-8')) + len(end_mak.encode('utf-8'))
                    length = format(lens,'%dd'%beg_mak)
                    print length,len(length)
                    c.sendall(length + cmdout.encode('utf-8') + cmderr.encode('utf-8') + end_mak.encode('utf-8'))
                except Exception as e:
                    print 'Wrong: %s ' % e
                    break
                else:
                    print 'End session.'
            c.close()
        s.close()

if __name__ == "__main__":
    t_pd_ser = threading.Thread(target=pd_socket_server)
    t_pd_ser.setDaemon(True)
    t_pd_ser.start()
    while True:
        k_in = raw_input()
        if k_in == 'Q':
            break
    #pd_socket_server()