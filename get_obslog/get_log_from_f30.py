#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os,sys,time,datetime,psycopg2, paramiko

def con_db():
    host = '10.0.10.236'
    port = '5432'
    user = 'yunwei'
    password = 'gwac1234'
    database = 'gwacyw'
    try:
        db = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
        return db
    except:
        print "Connection to the DB is wrong."
        return False

def sql_get(sql, n=1):
    db = con_db()
    if db:
        cur = db.cursor()
        try:
            if n == 0:###仅执行sql
                cur.execute(sql)
                db.commit()
            else:
                cur.execute(sql)
                rows = cur.fetchall()
                return rows
        except psycopg2.Error, e:
            print e
            return False
        finally:   
            db.close()
    else:
        print "\nMake waves 1 !"

def con_ssh(cmd):
    ip, username, passwd =  ["190.168.1.207","ccduser","x"][:3]
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
    ssh.connect(ip, 22, username, passwd, timeout=10)
    stdin, stdout, stderr = ssh.exec_command(cmd,get_pty=True)
    out = stdout.readlines()
    ssh.close()
    return out

def get_rank(obj_name):
    sql = "select objrank from object_list_all where obj_name='" + obj_name + "'"
    res = sql_get(sql)
    if res:
        return ''.join(res[0])
    else:
        return 0

print '\n'
obs_date = '2019-04-26'
date_now = time.strftime("%Y%m%d", time.localtime(time.time()))
print 'date_now:'+date_now
f = open('get-f30-log_%s.txt' % date_now, 'w+')
f.write('# obj_name objrank begin_time end_time group_id(unit_id)\n')

recs = {}
print '\n'
dir = '/home/ccduser/data/Y2019/2019-04-26/SVOMMM'
cmd = 'dirname %s/*/* | uniq' % dir
res = con_ssh(cmd)
for dp in res:
    cmd = 'basename %s' % dp
    res = con_ssh(cmd)
    obj_name = ''.join(res).strip()
    #print obj_name
    #
    objrank = get_rank(obj_name)
    #
    obj_beg = obj_name + "*01.fit"
    obj_end = obj_name + "*03.fit"
    cmd = "~/software/wcstools-3.8.7/bin/gethead /home/ccduser/data/Y2019/%s/SVOMMM/%s/%s DATE-OBS" % (obs_date,obj_name,obj_beg)
    beg_t = ''.join(con_ssh(cmd)).strip()
    beg_t = time.strftime("%Y-%m-%dT%H:%M:%S",time.strptime(beg_t, "%Y-%m-%dT%H:%M:%S.%f"))
    cmd = "~/software/wcstools-3.8.7/bin/gethead /home/ccduser/data/Y2019/%s/SVOMMM/%s/%s DATE-OBS" % (obs_date,obj_name,obj_end)
    end_t = ''.join(con_ssh(cmd)).strip()
    cmd = "~/software/wcstools-3.8.7/bin/gethead /home/ccduser/data/Y2019/%s/SVOMMM/%s/%s EXPTIME" % (obs_date,obj_name,obj_end)
    exp = ''.join(con_ssh(cmd)).strip()
    end_t = time.strftime("%Y-%m-%dT%H:%M:%S",time.localtime(time.mktime(time.strptime(end_t, "%Y-%m-%dT%H:%M:%S.%f")) + int(exp) + 3))
    print obj_name, objrank,beg_t, end_t,'XL003_001'
    rec = "%s   %s   %s   %s   XL003_001" % (obj_name, objrank,beg_t, end_t)
    recs[beg_t] = rec
print '\n\n'
bts = recs.keys()
bts.sort()
print bts
print '\n\n'
for bt in bts:
    print recs[bt]
    f.write('\n'+recs[bt]+'\n')
f.close()
print '\n'

