#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re,sys,json,time,datetime,paramiko
import numpy as np

# load the adapter
import psycopg2
# load the psycopg extras module
import psycopg2.extras
try:
    sys.path.append("./ext/")
    from dd2dms import dd2dms
    from dd2hms import dd2hms
    from func_gwac_too_image_status_query import func_gwac_too_image_status_query
    from first_and_last_image import first_and_last_image
except:
    print("please install sidereal code ")

if not sys.argv[1:]:
    sys.argv += ['S190425z','20190425',"2019-04-25T10:00:00", "2019-04-27T02:00:00"]

objsr = sys.argv[1]
t_str = sys.argv[2]
utc_datetime_begin_str_T = sys.argv[3]
utc_datetime_end_str_T = sys.argv[4]

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

objs = []

date_now = time.strftime("%Y%m%d", time.localtime(time.time()))
print 'date_now:'+date_now
lf = open("get-gwac-log_from-%s_at-%s.txt" % (t_str,date_now), "w+")
#lf.write("\ndate_now:%s\n" % date_now)

date_k = datetime.datetime.strptime(t_str, "%Y%m%d")

dt_b = date_k.strftime("%Y%m%d")
dt_e = (date_k + datetime.timedelta(days= 1)).strftime("%Y%m%d")
date = [dt_b,dt_e]
# date = ['20190425','20190426']
# dt_b,dt_e = date[:]
print date
for item in date:
    log = '/var/log/gtoaes/gtoaes_%s.log' % item
    print log
    #lf.write(log+'\n')
    cmd = "cat %s | grep -a '.*plan.*goes running on.*'" % log
    res = con_ssh(cmd)
    if res:
        for item in res:
            item = item.strip()
            obj = re.search(r'plan<(.*?)>', item).group(1)
            objs.append(obj)
print '\n\n'
objs1 = []
objs2 = []
for obj in objs:
    if obj not in objs2:
        objs1.append(int(obj))
        objs2.append(obj)
print objs1
#lf.write("\n\n"+','.join(objs2)+'\n\n')
print '\n\n'
for obj in objs2:
    b_t = ''
    e_t = ''
    u_id_b = ''
    u_id_e = ''
    sql = "select obj_name,objrank,objra,objdec,objsour from object_list_all where obj_id = '" + obj + "'"
    #print sql
    res_x = sql_get(sql)
    if res_x:
        print res_x[0][0],res_x[0][1],res_x[0][2],res_x[0][3],res_x[0][4]
        #lf.write("\n%s %s %s %s %s\n" % (res_x[0][0],res_x[0][1],res_x[0][2],res_x[0][3],res_x[0][4]))
        if res_x[0][4] and objsr in res_x[0][4]:
            for item_t in date:
                log = '/var/log/gtoaes/gtoaes_%s.log' % item_t
                cmd = "cat %s | grep -a '.*plan<%s>.*'" % (log, obj)
                res = con_ssh(cmd)
                if res:
                    print log
                    #lf.write("\n%s %s\n" % (log,item_t))
                    mark = 0
                    for item in res:
                        print item.strip()
                        bk_t = re.search(r'^(\d\d:\d\d:\d\d) >>',item)
                        if bk_t and item_t == dt_b and bk_t.group(1) < '12:00:00':
                            print '\nBreak in beg'
                            #lf.write('\nBreak in beg\n')
                            break
                        if bk_t and item_t == dt_e and bk_t.group(1) > '12:00:00':
                            print '\nBreak in end'
                            #lf.write('\nBreak in end\n')
                            break
                        res_re_b = re.search(r'(\d\d:\d\d:\d\d) >> plan<.*?> goes running on <001:(.*?)>',item)
                        if res_re_b:
                            b_t = item_t +'T'+ res_re_b.group(1)
                            b_t = datetime.datetime.utcfromtimestamp(time.mktime(time.strptime(b_t, "%Y%m%dT%H:%M:%S"))).strftime("%Y-%m-%dT%H:%M:%S")
                            u_id_b = res_re_b.group(2)
                            mark = 1
                        res_re_e = re.search(r'(\d\d:\d\d:\d\d) >> plan<.*?> on <001:(.*?)> is over',item)
                        if res_re_e:
                            e_t = item_t +'T'+ res_re_e.group(1)
                            e_t = datetime.datetime.utcfromtimestamp(time.mktime(time.strptime(e_t, "%Y%m%dT%H:%M:%S"))).strftime("%Y-%m-%dT%H:%M:%S")
                            u_id_e = res_re_e.group(2)
                            if mark == 1:
                                mark = 2
                        #lf.write('\n'+item)
                        if mark == 2 and (b_t and e_t and u_id_b and u_id_e) and e_t > b_t and u_id_b == u_id_e:
                            print "\n%s %s %s %s %s %s %s %s\n" % (res_x[0][0],res_x[0][1],res_x[0][2],res_x[0][3],res_x[0][4],b_t,e_t,u_id_b)
                            lf.write("\n%s %s %s %s %s %s %s %s\n" % (res_x[0][0],res_x[0][1],res_x[0][2],res_x[0][3],res_x[0][4],b_t,e_t,u_id_b))
            lf.write('\n')
            print '\n'
        else:
            if res_x[0][4]:
                print '\nNot obj : %s' % res_x[0][4]
            else:
                print '\nNot obj'
            #lf.write('\nNot obj\n')
lf.close()

########################
print '\nGOING...\n'

location = 'xinglong'
if location == 'xinglong':
    configuration_file = 'configuration_xl.dat'
elif location == 'beijing':
    configuration_file = 'configuration_bj.dat'

utc_datetime_begin = datetime.datetime.strptime(utc_datetime_begin_str_T, '%Y-%m-%dT%H:%M:%S')
utc_datetime_end = datetime.datetime.strptime(utc_datetime_end_str_T, '%Y-%m-%dT%H:%M:%S')
utc_datetime_begin_str = utc_datetime_begin_str_T.replace('T',' ')
utc_datetime_end_str = utc_datetime_end_str_T.replace('T',' ')

currenttime_time  = time.gmtime()
today_str = time.strftime("%Y-%m-%d", currenttime_time)
date_datetime = datetime.datetime.strptime(today_str, '%Y-%m-%d')
DB_switch_datetime  = date_datetime - datetime.timedelta(hours=15)

Grid_ID = []
Field_ID = []
Pointing_RA = []
Pointing_DEC = []
with open("get-gwac-log_from-%s_at-%s.txt" % (t_str,date_now), "r") as file_to_read:
#with open('log.txt', 'r') as file_to_read:
    for line in file_to_read.readlines():
        line = line.strip()
        if line:
            name = line.split()[0]
            #print name
            gid = name.split('_')[0]
            fid = name.split('_')[1]
            pra = line.split()[2]
            pdec = line.split()[3]
            Grid_ID.append(gid)
            Field_ID.append(fid)
            Pointing_RA.append(pra)
            Pointing_DEC.append(pdec)
print Grid_ID,Field_ID,'\n'
u,indices = np.unique(Field_ID, return_index=True)

if len(u) > 0:
    lm = open("infs_of-%s_at-%s.txt" % (t_str,date_now), "w+")
    print("Grid_ID,Field_ID,trigger_id,Pointing_RA,Pointing_DEC,B_UT , E_UT, M_coor_ra_deg, M_coor_dec_deg, M_coor_ra, M_coor_dec, Image_coor_ra_deg, Image_coor_dec_deg, Image_coor_ra, Image_coor_dec, CCD_ID, CCD_TYPE")  
    lm.write("Grid_ID,Field_ID,trigger_id,Pointing_RA,Pointing_DEC,B_UT , E_UT, M_coor_ra_deg, M_coor_dec_deg, M_coor_ra, M_coor_dec, Image_coor_ra_deg, Image_coor_dec_deg, Image_coor_ra, Image_coor_dec, CCD_ID, CCD_TYPE\n")
    for nin in indices:
        data = func_gwac_too_image_status_query(configuration_file,utc_datetime_begin,utc_datetime_begin,utc_datetime_end,float(Pointing_RA[nin]),float(Pointing_DEC[nin]))
        if len(data[0]) >= 1:
            all_list = first_and_last_image(data)[0] 
            mark_list = first_and_last_image(data)[1]
            p = 0
            for k in all_list:
                B_UT = k['B_UT'].strftime('%Y-%m-%d %H:%M:%S')
                E_UT = k['E_UT'].strftime('%Y-%m-%d %H:%M:%S')
                M_coor_ra_deg = str(mark_list[p][0])
                M_coor_dec_deg = str(mark_list[p][1])
                M_coor_ra = dd2hms(float(M_coor_ra_deg))
                M_coor_dec = dd2dms(float(M_coor_dec_deg))
                Image_coor_ra_deg = str(k['Image_RA'])
                Image_coor_dec_deg = str(k['Image_DEC'])
                Image_coor_ra = dd2hms(float(Image_coor_ra_deg))
                Image_coor_dec = dd2dms(float(Image_coor_dec_deg))
                CCD_ID = str(k['CCD_ID'])
                CCD_TYPE = str(k['CCD_TYPE'])
                outline = '%s %s %05.2f %05.2f %s %s %s %s %s %s %s %s %s %s %s %s' % (Grid_ID[nin],Field_ID[nin],float(Pointing_RA[nin]),float(Pointing_DEC[nin]),B_UT , E_UT, M_coor_ra_deg, M_coor_dec_deg, M_coor_ra, M_coor_dec, Image_coor_ra_deg, Image_coor_dec_deg, Image_coor_ra, Image_coor_dec, CCD_ID, CCD_TYPE)
                print(outline)
                lm.write('\n'+outline+'\n')
                p += 1
    lm.close()
