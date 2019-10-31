#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import os
import json
import time
import socket
import operator
import psycopg2
import paramiko
import sys
reload(sys)
sys.setdefaultencoding('utf8')

###
warn_bs = {}
###

def load_params(json_file):
    with open(json_file) as read_file:
        pd_params = json.load(read_file)
    return pd_params

def con_db():
    pd_params = load_params('./pd_params.json')
    try:
        db = psycopg2.connect(**pd_params['yunwei_db'])
    except psycopg2.Error :#as e:
        #print(e)
        return False
    else:
        return db

def sql_act(sql,n=1):
    global warn_bs
    db = con_db()
    if db:
        cur = db.cursor()
        try:
            cur.execute(sql)
            if n == 0:
                db.commit()
                cur.close()
                db.close()
                return
            else:
                rows = cur.fetchall()
                cur.close()
                db.close()
                return rows
        except psycopg2.Error as e:
            print "\nWARNING: Wrong with operating the db, %s " % str(e).strip()
            #warn_bs['db'] = "WARNING: Wrong with operating the db, " + str(e).strip()
            return False
    else:
        print "\nWARNING: Connection to the db is Error."
        #warn_bs['db'] = "WARNING: Connection to the db is Error."
        return 0

def pg_act(table,action,args=[]):
    if args:
        if action == 'delete':
            cond_keys = args[0].keys()
            conds = []
            for key in cond_keys:
                conds.append("='".join([key,args[0][key]]))
            cond = "' AND ".join(conds)
            sql = "DELETE FROM " + table + " WHERE " + cond + "'"
            sql_act(sql, 0)
        if action == "insert":
            #args = [{'obj_id':'1',}]
            rows = args[0].keys()
            vals = []
            for row in rows:
                vals.append(args[0][row])
            sql = "INSERT INTO " + table + " (" + ", ".join(rows) + ") VALUES ('" + "', '".join(vals) +"')"
            sql_act(sql, 0)
        if action == "update":
            #args = [{'obj_name':'x','obj_comp_time':'2019-01-01 00:00:00'},{'obj_id':'1','obs_stag':'sent'}]
            rows = args[0].keys()
            targs = []
            for row in rows:
                targs.append("='".join([row,args[0][row]]))
            targ = "' , ".join(targs)
            cond_keys = args[1].keys()
            conds = []
            for key in cond_keys:
                conds.append("='".join([key,args[1][key]]))
            cond = "' AND ".join(conds)
            sql = "UPDATE " + table + " SET " + targ + "' WHERE " + cond + "'"
            sql_act(sql, 0)
        if action == "select":
            #args = [['1','2'],{'obj_name':'x','obj_comp_time':'2019-01-01 00:00:00'}]
            rows = ','.join(args[0])
            if args[1]:
                cond_keys = args[1].keys()
                conds = []
                for key in cond_keys:
                    conds.append("='".join([key,args[1][key]]))
                cond = "' AND ".join(conds)
                cond += "'"
            else:
                cond = ''
            if len(args) > 2:
                cond_more = args[2]
            else:
                cond_more = ''
            sql = "SELECT " + rows + " FROM " + table + " WHERE " + cond + cond_more
            #print sql
            res = sql_act(sql)
            return res

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
        print '\n\nWARNING: [SOCKET] The cmd is null.\n\n' %e
        return 0

def con_ssh(ip, username, passwd, cmd, mode=1):
    global warn_bs
    if mode == 1:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        try:
            ssh.connect(hostname=ip, port=22, username=username, password=passwd, timeout=60)
        except Exception as e:
            print "\nWARNING: Connection of ssh is wrong, %s " % e
            #warn_bs['ssh'] = 'WARNING: Connection to %s by ssh is wrong!' % ip
            return 0
        else:
            stdin, stdout, stderr = ssh.exec_command(cmd,get_pty=True)
            out = stdout.readlines()
            # if out:
            #     print '1100'
            #     print out[0]
            err = stderr.readlines()
            # if err:
            #     print '0011'
            #     print err[0]
            ssh.close()
            if not out and not err:
                return 0
            if out and not err:
                return [1,out]
            if err and not out:
                return [2,err]
            if out and err:
                return [3,out,err]
    else:
        try:
            t = paramiko.Transport((ip, 22))
            t.connect(username=username, password=passwd)
            sftp = paramiko.SFTPClient.from_transport(t)
            #cmd = 'put/get t.t pd-tools/'
            cmd_strs = cmd.split(' ')
            cmd_cmd = cmd_strs[0]
            cmd_pth1 = cmd_strs[1]
            cmd_pth2 = cmd_strs[2]
            if cmd_cmd == 'put':
                sftp.put(cmd_pth1, cmd_pth2)
            if cmd_cmd == 'get':
                sftp.get(cmd_pth1, cmd_pth2)
            t.close()
            return 1
        except Exception as e:
            print '\nWARNING: The sftp is wrong, %s ' % e
            return 0

def get_ips_from_confs():
    ssh_socket_confs = load_params('./pd_params.json')['ssh_socket']
    ip_confs = [(ssh_socket_confs[i][0],i) for i in ssh_socket_confs.keys()]
    #print ip_confs
    ip_confs.sort(key=operator.itemgetter(0))
    ip_confs_sort_dic = dict(ip_confs)
    return ip_confs_sort_dic

def check_ser_socket(ip,mode=1,init=1):
    ip_confs_dic = get_ips_from_confs()
    ssh_socket_confs = load_params('./pd_params.json')['ssh_socket']
    if ip in ip_confs_dic.keys():
        key = ip_confs_dic[ip]
        #print key
        script = 'pd_socket_server.py'
        cmd = 'ps -ef | grep %s | grep -v grep' % script
        #print cmd
        sip, sun, spw = ssh_socket_confs[key][:3]
        #print sip, sun, spw
        res = con_ssh(sip, sun, spw, cmd)
        if res and res[0] == 1:
            retr = 1
        else:
            mode = 0
    else:
        return 0
    if mode == 0:
        if retr == 1:
            for item in res[1]:
                pid = item.split()[1]
                #print pid
                cmd = 'kill %s' % pid
                #print cmd
                res = con_ssh(sip, sun, spw, cmd)
                #print res
        if init == 0:
            port = ssh_socket_confs[key][3]
            cmd = 'iptables -I INPUT -p tcp --dport %s -j ACCEPT && service iptables save' % port
            root_pw = ssh_socket_confs[key][4]
            con_ssh(sip, 'root', root_pw, cmd)
        socket_ser_path = '/home/' + ssh_socket_confs[key][1] + '/pd-socket'
        #print socket_ser_path
        cmd = 'mkdir %s' % socket_ser_path
        #print cmd
        res = con_ssh(sip, sun, spw, cmd)
        #print res
        f1 = 'pd_socket_params.json'
        f2 = 'boot_server.sh'
        cmd = 'put %s %s/%s' % (script, socket_ser_path, script)
        #print cmd
        res = con_ssh(sip, sun, spw, cmd, mode=2)
        #print res
        cmd = 'put %s %s/%s' % (f1, socket_ser_path, f1)
        #print cmd
        res = con_ssh(sip, sun, spw, cmd, mode=2)
        #print res
        cmd = 'put %s %s/%s' % (f2, socket_ser_path, f2)
        #print cmd
        res = con_ssh(sip, sun, spw, cmd, mode=2)
        #print res
        cmd = 'cd %s && nohup sh %s %s > /dev/null 2>&1 ' % (socket_ser_path, f2, script)
        #print cmd
        res = con_ssh(sip, sun, spw, cmd)
        #print res
        cmd = 'ps -ef | grep %s | grep -v grep' % script
        sip, sun, spw = ssh_socket_confs[key][:3]
        res = con_ssh(sip, sun, spw, cmd)
        if res and res[0] == 1:
            return 1
        else:
            print '\nWARNING:The socket server of %s is not on-line' % ip
            return 0
    else:
        return retr

def check_ser_socket_background():
    while True:
        ip_confs_dic = get_ips_from_confs()
        for ip in ip_confs_dic.keys():
            check_ser_socket(ip)
        time.sleep(10)

def get_ser_config(group_id):
    if group_id == 'XL001':
        conf_key = 'GWAC_ser'
    if group_id == 'XL002':
        conf_key = 'F60_ser'
    if group_id == 'XL003':
        conf_key = 'F30_ser'
    ser_ip = load_params('./pd_params.json')['ssh_socket'][conf_key][0]
    ser_port = load_params('./pd_params.json')['ssh_socket'][conf_key][3]
    ser_conf_list = [ser_ip, ser_port]
    return ser_conf_list

def get_cam_config(cam_id):
    if cam_id == '1':
        conf_key = 'w60_cam'
    if cam_id == '2':
        conf_key = 'e60_cam'
    if cam_id == '3':
        conf_key = '30_cam'
    cam_ip = load_params('./pd_params.json')['ssh_socket'][conf_key][0]
    cam_port = load_params('./pd_params.json')['ssh_socket'][conf_key][3]
    cam_conf_list = [cam_ip, cam_port]
    return cam_conf_list


if __name__ == "__main__":
    #check_ser_socket()
    #print get_ips_from_confs()
    ip = '190.168.1.203'
    # xx = check_ser_socket(ip,0)
    # print xx
    cmd = 'ls /tmp *.log | sort'
    port = '33369'
    # res = pd_socket_client(ip,port,cmd)
    # print res
