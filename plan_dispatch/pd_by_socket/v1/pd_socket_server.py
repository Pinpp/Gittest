#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import socket
import subprocess


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
    with open(conf_file) as conf:
        pd_params = json.load(conf)
    socket_confs = pd_params['socket']
    ips = {i[0]:i[1] for i in socket_confs.values()}
    ip = get_host_ip()
    if not ip:
        return 0
    if ip in ips:
        port = ips[ip]
        return (ip,int(port))
    else:
        return 0
if __name__ == "__main__":
    conf_file = './pd_socket_params.json'
    socket_conf = get_ser_conf(conf_file)
    if socket_conf:
        s = socket.socket()
        s.bind(socket_conf)
        s.listen(5)
        while True:
            c, addr = s.accept()
            #print 'Address: ', addr
            while True:
                try:
                    msg = c.recv(1024)
                    #print msg
                    if not msg:
                        break
                    cmd = subprocess.Popen(msg.decode('utf-8'), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    cmdout = cmd.stdout.read()
                    cmderr = cmd.stderr.read()
                    lens = len(cmdout.encode('utf-8')) + len(cmderr.encode('utf-8'))
                    length = '%010d' % lens
                    #print length,len(length)
                    #c.sendall(length)
                    c.sendall(length + cmdout.encode('utf-8') + cmderr.encode('utf-8'))
                except Exception as e:
                    #print 'Something Wrong, %s ' % e
                    break
                #print 'End session.'
            c.close()
        s.close()