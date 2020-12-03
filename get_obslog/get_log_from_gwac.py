#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re,time,datetime,paramiko,psycopg2

def con_ssh(cmd):
    ip, username, passwd =  ["172.28.1.11","gwac","gwac1234"][:3]
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
    ssh.connect(ip, 22, username, passwd, timeout=10)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.readlines()
    ssh.close()
    return out

def con_db():
    host = '172.28.8.28'#10.0.10.236'
    port = '5432'
    user = 'yunwei'
    password = 'gwac1234'
    database = 'gwacyw'
    try:
        db = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
        return db
    except psycopg2.Error, e:
        print e
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

objs1 = []
objs2 = []
date_now = time.strftime("%Y%m%d", time.localtime(time.time()))
print 'date_now:'+date_now
lf = open("get-gwac-log_%s.txt" % date_now, "w+")
lf.write("\ndate_now:%s\n" % date_now)
dt1 = (datetime.datetime.now()).strftime("%Y%m%d")
dt2 = (datetime.datetime.now() + datetime.timedelta(days= -1)).strftime("%Y%m%d")
date = [dt2,dt1]
for item in date:
    log = '/var/log/gtoaes/gtoaes_%s.log' % item
    print log
    lf.write(log+'\n')
    cmd = "cat %s | grep -a '.*plan.*goes running on.*'" % log
    res = con_ssh(cmd)
    if res:
        for item in res:
            item = item.strip()
            obj = re.search(r'plan<(.*?)>', item).group(1)
            objs1.append(int(obj))
            objs2.append(obj)
print '\n\n'
print objs1
lf.write("\n\n"+','.join(objs2)+'\n\n')
print '\n\n'
for obj in objs2:
    b_t = ''
    e_t = ''
    u_id_b = ''
    u_id_e = ''
    sql = "select obj_name,objrank,objra,objdec from object_list_all where obj_id = '" + obj + "'"
    #print sql
    res_x = sql_get(sql)
    if res_x:
        print res_x[0][0],res_x[0][1],res_x[0][2],res_x[0][3]
        lf.write("\n%s %s %s %s\n" % (res_x[0][0],res_x[0][1],res_x[0][2],res_x[0][3]))
        for item_t in date:
            log = '/var/log/gtoaes/gtoaes_%s.log' % item_t
            cmd = "cat %s | grep -a '.*plan<%s>.*'" % (log, obj)
            res = con_ssh(cmd)
            if res:
                print log
                lf.write("\n%s %s\n" % (log,item_t))
                for item in res:
                    print item.strip()
                    bk_t = re.search('^(\d\d:\d\d:\d\d) >>',item)
                    if bk_t and item_t == dt2 and bk_t.group(1) < '12:00:00':
                        print '\nBreak'
                        lf.write('\nBreak\n')
                        break
                    res_re_b = re.search('(\d\d:\d\d:\d\d) >> plan<.*?> goes running on <001:(.*?)>',item)
                    if res_re_b:
                        b_t = item_t +'T'+ res_re_b.group(1)
                        u_id_b = res_re_b.group(2)
                    res_re_e = re.search('(\d\d:\d\d:\d\d) >> plan<.*?> on <001:(.*?)> is over',item)
                    if res_re_e:
                        e_t = item_t +'T'+ res_re_e.group(1)
                        u_id_e = res_re_e.group(2)
                    lf.write('\n'+item)
        if (b_t and e_t and u_id_b and u_id_e) and e_t > b_t and u_id_b == u_id_e:
            print "\n%s %s %s %s %s %s %s\n" % (res_x[0][0],res_x[0][1],res_x[0][2],res_x[0][3],b_t,e_t,u_id_b)
            lf.write("\n%s %s %s %s %s %s %s\n" % (res_x[0][0],res_x[0][1],res_x[0][2],res_x[0][3],b_t,e_t,u_id_b))
        lf.write('\n')
        print '\n'
lf.close()