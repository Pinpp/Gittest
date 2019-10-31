#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import re, os, sys, time, datetime, threading, operator
import paramiko, psycopg2
from pydash import at
from communication_client import Client
from ObservationPlanUpload import ObservationPlanUpload
from ToP_obs_plan_insert_DB import insert_to_ba_db,update_to_ba_db,update_pointing_lalalert
from pd_followup import pd_followup


###
client = Client('plan_dispatch')
xclient = Client('object_generator')
pd_log_tab = 'pd_log_current'
running_list_cur = 'object_running_list_current'
gwac_init = ['002','004']
f60_init = ['001']
f30_init = ['001']
send_objs = []
###

def get_ser_config(type):
    if type == 'XL001':
        ser_ip = '172.28.1.11'
        ser_un = 'gwac'
        ser_pw = 'gwac1234'
    if type == 'XL002':
        ser_ip = '190.168.1.203'
        ser_un = 'w60ccd'
        ser_pw = 'x'
    if type == 'XL003':
        ser_ip = '190.168.1.207'
        ser_un = 'ccduser'
        ser_pw = 'x'
    ser_con_list = [ser_ip, ser_un, ser_pw]
    return ser_con_list

def get_cam_config(cam_id):
    if cam_id == '1':
        cam_ip = '190.168.1.203'
        cam_un = 'w60ccd'
        cam_pw = 'x'
    if cam_id == '2':
        cam_ip = '190.168.1.201'
        cam_un = 'e60ccd'
        cam_pw = 'x'
    if cam_id == '3':
        cam_ip = '190.168.1.207'
        cam_un = 'ccduser'
        cam_pw = 'x'
    cam_con_list = [cam_ip, cam_un, cam_pw]
    return cam_con_list

def con_ssh(ip, username, passwd, cmd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
    try:
        ssh.connect(hostname=ip, port=22, username=username, password=passwd, timeout=30)
    except:
        print "\nWARNING: Connection of ssh is wrong!"
    else:
        stdin, stdout, stderr = ssh.exec_command(cmd, get_pty=True)
        out = stdout.readlines()
        ssh.close()
        return out
 
def con_db():
    host = '172.28.8.28'#'10.0.10.236'
    database = 'gwacyw'
    user = 'yunwei'
    password = 'gwac1234'
    try:
        db = psycopg2.connect(host=host, port=5432, user=user, password=password, database=database)
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
                time.sleep(1.5)
            else:
                cur.execute(sql)
                rows = cur.fetchall()
                return rows
        except psycopg2.Error as e:
            print "\nWARNING: Wrong with operating the db, %s " % str(e).strip()
            return False
        finally:
            cur.close()
            db.close()
    else:
        print "\nWARNING: Connection to the db is Error."

def pg_db(table,action,args=[]):
    if args:
        if action == 'delete':
            cond_keys = args[0].keys()
            conds = []
            for key in cond_keys:
                conds.append("='".join([key,args[0][key]]))
            cond = "' AND ".join(conds)
            sql = "DELETE FROM " + table + " WHERE " + cond + "'"
            sql_get(sql, 0)
        if action == "insert":
            #args = [{'obj_id':'1',}]
            rows = args[0].keys()
            vals = []
            for row in rows:
                vals.append(args[0][row])
            #sql = "INSERT INTO " + table + " ('" + "', '".join(rows) + "') VALUES ('" + "', '".join(vals) +"')"
            sql = "INSERT INTO " + table + " (" + ", ".join(rows) + ") VALUES ('" + "', '".join(vals) +"')"
            #print sql
            sql_get(sql, 0)
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
            #print sql
            sql_get(sql, 0)
        if action == "select":
            #args = [['1','2'],{'obj_name':'x','obj_comp_time':'2019-01-01 00:00:00'}]
            rows = ','.join(args[0])
            cond_keys = args[1].keys()
            conds = []
            for key in cond_keys:
                conds.append("='".join([key,args[1][key]]))
            cond = "' AND ".join(conds)
            sql = "SELECT " + rows + " FROM " + table + " WHERE " + cond + "'"
            #print sql
            res = sql_get(sql)
            return res

def check_ser(type):
    if type in ['XL002','XL003']:
        ser_ip, ser_un, ser_pw = get_ser_config(type)[0:3]
        cmd = 'ps -ef | grep gftservice | grep -v grep'
        res = con_ssh(ser_ip, ser_un, ser_pw, cmd)
        if res:
            return True
        else:
            print "\nWARNING: The gftservice of %s is Error." % type
            return False
    if type == 'XL001':
        ser_ip, ser_un, ser_pw = get_ser_config(type)[0:3]
        cmd = 'ps -ef | grep gtoaes | grep -v grep'
        res = con_ssh(ser_ip, ser_un, ser_pw, cmd)
        if res:
            return True
        else:
            print "\nWARNING: The gtoaes of %s is Error." % type
            return False

def check_cam(cam_id):
    cam_ip, cam_un, cam_pw = get_cam_config(cam_id)[0:3]
    cmd = 'ps -ef | grep camagent | grep -v grep'
    res = con_ssh(cam_ip, cam_un, cam_pw, cmd)
    if res:
        return True
    else:
        print "\nThe camagent of %s is Error." % cam_id
        return False

def get_obj_inf(obj):            ### Get the infomation.
    infs = {}
    sql = "select column_name from information_schema.columns where table_schema='public' and table_name='object_list_all'"
    cols = sql_get(sql)
    #print cols
    sql = "SELECT * FROM object_list_all WHERE obj_id='" + obj + "'"
    infs_list = sql_get(sql)
    #print infs_list
    if cols and infs_list:
        for i in range(1,len(cols)):
            infs[cols[i][0]] = str(infs_list[0][i])
        if infs['filter'] == 'clear':
            if infs['group_id']  == "XL002":
                infs['filter'] = "Lum"
            if infs['group_id']  == "XL003":
                infs['filter'] = "R"
                #print "\nWARNING: The filter of %s input Error, using filter R." % obj
                print "%s WARNING: Filter Error when get_obj_infs, using filter R." % obj
        if len(infs['obj_name']) > 20:
            if infs['group_id'] in ['XL002', 'XL003']:
                #print "\nWARNING: The name of %s is too long, attention please!" % obj
                infs['obj_name'] = infs['obj_name'][:20]
    # infs = {}
    # sql = "SELECT group_id, unit_id, obj_name, obs_type, obs_stra, objtype, objsour, observer, objra, objdec, objepoch, objerror, imgtype, filter, expdur, delay, frmcnt, priority, run_name, objrank, mode, ra_shift, dec_shift FROM object_list_all WHERE obj_id='" + obj + "'"
    # infs_list = sql_get(sql)[0]
    # group_id, unit_id, obj_name, obs_type, obs_stra, objtype, objsour, observer, objra, objdec, objepoch, objerror, imgtype, filter, expdur, delay, frmcnt, priority, run_name, objrank, mode, ra_shift, dec_shift = infs_list[:]#[:20]
    # if filter == "clear":
    #     if group_id  == "XL002":
    #         filter = "Lum"
    #     if group_id  == "XL003":
    #         filter = "R"
    #         print "\nWARNING: The filter of %s input Error, using filter R." % obj
    # if len(obj_name) > 20:
    #     if group_id in ['XL002', 'XL003']:
    #         #print "\nWARNING: The name of %s is too long, attention please!" % obj
    #         obj_name = obj_name[:20]
    while True:
        sql = "SELECT tw_begin, tw_end FROM object_list_current WHERE obj_id='" + obj + "'"
        res = sql_get(sql)
        if res and len(res[0]) == 2:
            begin_time, end_time = res[0][0:2]
            if begin_time and end_time:
                infs.update(begin_time=begin_time, end_time=end_time)
                break
    #infs.update(group_id=group_id, unit_id=unit_id, obj_name=obj_name, obs_type=obs_type, obs_stra=obs_stra, objtype=objtype, objsour=objsour, observer=observer, objra=str(objra), objdec=str(objdec), objepoch=str(objepoch), objerror=objerror, imgtype=imgtype, filter=filter, expdur=str(expdur), delay=str(delay), frmcnt=str(frmcnt), priority=str(priority), run_name=str(run_name), objrank=objrank, mode=mode, ra_shift=str(ra_shift), dec_shift=str(dec_shift), begin_time=begin_time, end_time=end_time)
    return infs

def Ra_to_h(ra):
    if ':' in ra:
        RaList = map(float,ra.split(':'))
        return str(RaList[0] + RaList[1]/60 + RaList[2]/3600)
    elif abs(float(ra)) <= 360:
        a = float(ra)/15
        return str(a)
    else:
        exit('\nWARNING: Wrong RA.')

def send_cmd(obj,unit_id):
    infs = get_obj_inf(obj)
    group_id = infs["group_id"]
    ###
    obj_name, observer, obs_type, obs_stra, objra, objdec, objepoch, objerror, imgtype, filter, expdur, delay, frmcnt, priority, run_name, begin_time, end_time = at(infs, 'obj_name', 'observer', 'obs_type', 'obs_stra', 'objra', 'objdec', 'objepoch', 'objerror', 'imgtype', 'filter', 'expdur', 'delay', 'frmcnt', 'priority', 'run_name', 'begin_time', 'end_time')
    ###
    date_cur = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    send_beg_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    time.sleep(1.5)
    #print send_beg_time
    if group_id == 'XL001':
        #unit_id = unit_id
        objsource = infs['objsour']
        ser_ip, ser_un, ser_pw = get_ser_config(group_id)[0:3]
        op_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        grid_id = 'G0014'
        try:
            strs = obj_name.split('_')
        except:
            field_id = obj_name
        else:
            grid_id = strs[0]
            field_id = strs[1]
        if frmcnt != '-1':
            m1 = float(expdur)
            m2 = float(delay)
            n = int(frmcnt)
            b_time = op_time
            e_time = datetime.datetime.utcfromtimestamp(time.time()+(m1+m2)*n).strftime("%Y-%m-%dT%H:%M:%S")
        else:
            #b_time = "0000-00-00T00:00:00"
            #e_time = "0000-00-00T00:00:00"
            b_time = begin_time.replace("/", "-")
            b_time = begin_time.replace(" ", "T")
            e_time = end_time.replace("/", "-")
            e_time = end_time.replace(" ", "T")
        cmd = "tryclient 'append_gwac Op_sn=%s, Op_time=%s, Op_type=obs,Group_ID=001, Unit_ID=%s, ObsType=%s, Grid_ID=%s, Field_ID=%s, Obj_ID=%s,RA=%s, DEC=%s, Epoch=2000, ObjRA=%s, ObjDEC=%s, ObjEpoch=%s, ObjError=%s, ImgType=%s,expdur=%s, delay=%s, frmcnt=%s, priority=%s, begin_time=%s, end_time=%s Pair_ID=0'" % (obj,op_time,unit_id,obs_type,grid_id,field_id,objsource,objra,objdec,objra,objdec,objepoch,objerror,imgtype,expdur,delay,frmcnt,priority,b_time,e_time)
        #print cmd
        con_ssh(ser_ip, ser_un, ser_pw, cmd)
        #######
        pg_db(pd_log_tab,'insert', [{'obj_id':obj,'obj_name':obj_name,'priority':priority,'group_id':group_id,'unit_id':unit_id,'date_cur':date_cur,'obs_stag':'sent'}])
        #######
        time.sleep(5)
        send_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        #print send_end_time
    if group_id == 'XL002':
        #unit_id = '001'
        objsource = infs['objsour']
        op_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        ser_ip, ser_un, ser_pw = get_ser_config(group_id)[0:3]
        object_ra = Ra_to_h(objra)
        if objsource == 'GWAC_followup':
            run_name = infs['run_name']
            cmd = "append_plan %s %s && append_object %s %s %s %s 4 %s %s %s %s -1 -1 %s && append_plan gwac default " % (observer, obs_type, obj_name, object_ra, objdec, objepoch, expdur, frmcnt, filter, priority, run_name)
        else:
            cmd = "append_plan %s %s && append_object %s %s %s %s 4 %s %s %s %s && append_plan gwac default " % (observer, obs_type, obj_name, object_ra, objdec, objepoch, expdur, frmcnt, filter, priority)
        con_ssh(ser_ip, ser_un, ser_pw, cmd)
        #####
        pg_db(pd_log_tab,'insert', [{'obj_id':obj,'obj_name':obj_name,'priority':priority,'group_id':group_id,'unit_id':unit_id,'date_cur':date_cur,'obs_stag':'sent'}])
        #####
        time.sleep(3)
        send_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    if group_id == 'XL003':
        #unit_id = '001'
        op_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        ser_ip, ser_un, ser_pw = get_ser_config(group_id)[0:3]
        obs_stra = infs['obs_stra']
        if obs_stra == 'dithering':
            ra_shift = infs['ra_shift']
            dec_shift = infs['dec_shift']
            if ra_shift == 'None':
                ra_shift = 0
            if dec_shift == 'None':
                dec_shift = 0
            for i in range(int(frmcnt)):
                objra = float(objra) + float(ra_shift)
                objdec = float(objdec) + float(dec_shift)
                object_ra = Ra_to_h(str(objra))
                cmd = "append_plan %s %s && append_object %s %s %s %s 4 %s %s %s %s && append_plan gwac default " % (observer, obs_type, obj_name, object_ra, str(objdec), objepoch, expdur, '1', filter, priority)
                con_ssh(ser_ip, ser_un, ser_pw, cmd)
                if i == 0:
                    #print cmd
                    time.sleep(1.5)
                    send_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                    time.sleep(1.5)
        else:
            object_ra = Ra_to_h(objra)
            cmd = "append_plan %s %s && append_object %s %s %s %s 4 %s %s %s %s && append_plan gwac default " % (observer, obs_type, obj_name, object_ra, objdec, objepoch, expdur, frmcnt, filter, priority)
            con_ssh(ser_ip, ser_un, ser_pw, cmd)
            time.sleep(3)
            send_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        #####
        pg_db(pd_log_tab,'insert', [{'obj_id':obj,'obj_name':obj_name,'priority':priority,'group_id':group_id,'unit_id':unit_id,'date_cur':date_cur,'obs_stag':'sent'}])
        #####
    return [send_beg_time, send_end_time]

def send_db_in_beg(obj):
    infs = get_obj_inf(obj)
    ###
    group_id, obj_name, objsource, observer, obs_type, obs_stra, objra, objdec, objepoch, objerror, imgtype, filter, expdur, delay, frmcnt, priority, run_name, begin_time, end_time = at(infs, 'group_id', 'obj_name', 'objsour', 'observer', 'obs_type', 'obs_stra', 'objra', 'objdec', 'objepoch', 'objerror', 'imgtype', 'filter', 'expdur', 'delay', 'frmcnt', 'priority', 'run_name', 'begin_time', 'end_time')
    ###
    unit_id = 'None'
    res = pg_db(pd_log_tab,'select',[['obj_sent_time','unit_id'],{'obj_id':obj,'obs_stag':'sent'}])
    if res:
        obj_sent_time, unit_id = res[0][:]
        b_time = datetime.datetime.utcfromtimestamp(time.mktime(time.strptime(obj_sent_time, "%Y-%m-%d %H:%M:%S"))).strftime("%Y-%m-%dT%H:%M:%S")
    else:
        print '\n######WARNING: There is no record in send_db_in_beg of %s.' % obj
        b_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    ###
    op_time = b_time
    ###
    if group_id == 'XL001':
        grid_id = 'G0014'
        try:
            strs = obj_name.split('_')
        except:
            field_id = obj_name
        else:
            grid_id = strs[0]
            field_id = strs[1]
        if frmcnt != '-1':
            m1 = float(expdur)
            m2 = float(delay)
            n = int(frmcnt)
            b_time = op_time
            e_time = datetime.datetime.utcfromtimestamp(time.time()+(m1+m2)*n).strftime("%Y-%m-%dT%H:%M:%S")
        else:
            #b_time = "0000-00-00T00:00:00"
            #e_time = "0000-00-00T00:00:00"
            b_time = begin_time.replace("/", "-")
            b_time = begin_time.replace(" ", "T")
            e_time = end_time.replace("/", "-")
            e_time = end_time.replace(" ", "T")
        ###
        uploadUrl = 'http://172.28.8.8/gwebend/observationPlanUpload.action'
        opTime = op_time.replace('T',' ')
        beginTime = b_time.replace('T',' ')
        endTime = e_time.replace('T',' ')
        opSn,opType,groupId,unitId,obsType,gridId,fieldId,objId,ra,dec,epoch,objRa,objDec,objEpoch,objError,imgType,expusoreDuring,delay,frameCount,priority,pairId = [obj,'obs','001',unit_id,obs_type,grid_id,field_id,objsource,objra,objdec,objepoch,objra,objdec,objepoch,objerror,imgtype,str(int(float(expdur))),str(int(float(delay))),frmcnt,priority,'0'][:]
        tplan = ObservationPlanUpload(uploadUrl, opSn,opTime,opType,groupId,unitId,obsType,gridId,fieldId,objId,ra,dec,epoch,objRa,objDec,objEpoch,objError,imgType,expusoreDuring,delay,frameCount,priority,beginTime,endTime,pairId)
        tplan.sendPlan()
        tx = [uploadUrl, opSn,opTime,opType,groupId,unitId,obsType,gridId,fieldId,objId,ra,dec,epoch,objRa,objDec,objEpoch,objError,imgType,expusoreDuring,delay,frameCount,priority,beginTime,endTime,pairId]
        lf.write('\n#####'+' ,'.join(tx)+'\n')
        ###
        cmd_n = 'append_gwac'
        pg_db(running_list_cur,'insert',[{'cmd':cmd_n,'op_sn':obj,'op_time':op_time,'op_type':'obs','obj_id':obj,'obj_name':obj_name,'observer':observer,\
            'objra':objra,'objdec':objdec,'objepoch':objepoch,'objerror':objerror,'group_id':group_id,'unit_id':unit_id,'obstype':obs_type,\
                'obs_stra':obs_stra,'grid_id':grid_id,'field_id':field_id,'ra':objra,'dec':objdec,'imgtype':imgtype,'filter':filter,'expdur':expdur,'delay':delay,\
                    'frmcnt':frmcnt,'priority':priority,'begin_time':b_time,'end_time':e_time,'run_name':run_name,'pair_id':'0','mode':'observation','note':''}])
        ###
        #beginTime = b_time.replace('T',' ')
        #endTime = e_time.replace('T',' ')
        insert_to_ba_db(objsource,obj,group_id,unit_id,filter,grid_id,field_id,objra,objdec,obs_stra,beginTime,endTime,expdur,'observation','received')
        ###
    # if group_id in ['XL002','XL003']:
    #     #unit_id = '001'
    #     e_time = "0000-00-00T00:00:00"
    #     ###
    #     cmd_n = 'append_object'
    #     pg_db(running_list_cur,'insert',[{'cmd':cmd_n,'op_time':op_time,'op_type':'obs','obj_id':obj,'obj_name':obj_name,'observer':observer,\
    #         'objra':objra,'objdec':objdec,'objepoch':objepoch,'objerror':objerror,'group_id':group_id,'unit_id':unit_id,'obstype':obs_type,\
    #             'obs_stra':obs_stra,'ra':objra,'dec':objdec,'imgtype':imgtype,'filter':filter,'expdur':expdur,'delay':delay,\
    #                 'frmcnt':frmcnt,'priority':priority,'begin_time':b_time,'end_time':e_time,'run_name':run_name,'pair_id':'0','mode':'observation','note':''}])
    #     ###
    if group_id in ['XL002','XL003']:
        ##
        #unit_id = '001'
        e_time = "0000-00-00T00:00:00"
        ##
        cmd_n = 'append_object'
        pg_db(running_list_cur,'insert',[{'cmd':cmd_n,'op_time':op_time,'op_type':'obs','obj_id':obj,'obj_name':obj_name,'observer':observer,\
            'objra':objra,'objdec':objdec,'objepoch':objepoch,'objerror':objerror,'group_id':group_id,'unit_id':unit_id,'obstype':obs_type,\
                'obs_stra':obs_stra,'ra':objra,'dec':objdec,'imgtype':imgtype,'filter':filter,'expdur':expdur,'delay':delay,\
                    'frmcnt':frmcnt,'priority':priority,'begin_time':b_time,'end_time':e_time,'run_name':run_name,'pair_id':'0','mode':'observation','note':''}])
        ###
        try:
            objsour_word = objsource.split('_')
            trigger_type = objsour_word[0]
            version = objsour_word[1]
            trigger = objsour_word[2]
        except:
            pass
        else:
            if trigger_type == 'GW':
                if 'G0' in obj_name:
                    try:
                        strs = obj_name.split('_')
                    except:
                        grid_id = 'G0000'
                        field_id = obj_name
                    else:
                        grid_id = strs[0]
                        field_id = strs[1]
                else:
                    grid_id = 'G0000'
                    field_id = obj_name
                ###
                beginTime = b_time.replace('T',' ')
                endTime = e_time.replace('T',' ')
                insert_to_ba_db(objsource,obj,group_id,unit_id,filter,grid_id,field_id,objra,objdec,obs_stra,beginTime,endTime,expdur,'observation','received')
                ###
                sql = 'select "Op_Obj_ID" from trigger_obj_field_op_sn where "Trigger_ID"=' + "'" + trigger + "'" + ' and "Serial_num"=' + "'" + version + "'" + ' and "Obj_ID"=' + "'" + obj_name + "'"
                res = sql_get(sql)
                if res:
                    objs = []
                    for i in res:
                        objs.append(i[0])
                    objs = list(set(objs))
                    if len(objs) == 1:
                        try:
                            obj_nms = objs[0].split('|')
                        except:
                            obj_nm = res[0]
                            if 'G0' in obj_nm:
                                try:
                                    strs = obj_nm.split('_')
                                except:
                                    grid_id = 'G0000'
                                    field_id = obj_nm
                                else:
                                    grid_id = strs[0]
                                    field_id = strs[1]
                            else:
                                grid_id = 'G0000'
                                field_id = obj_nm
                            sql = 'select objrank from object_list_all where obj_name=' + "'" + obj_nm + "'" + 'and objsour=' + "'" + objsource + "'"
                            res = sql_get(sql)
                            if res:
                                objrank = res[0][0]
                            ###
                            if group_id == 'XL002':
                                name_telescope = 'F60'
                            if group_id == 'XL003':
                                name_telescope = 'F30'
                            update_pointing_lalalert(trigger,name_telescope,grid_id,field_id,objrank,'Received @ GWAC')
                            ###
                        else:
                            for obj_nm in obj_nms:
                                if 'G0' in obj_nm:
                                    try:
                                        strs = obj_nm.split('_')
                                    except:
                                        grid_id = 'G0000'
                                        field_id = obj_nm
                                    else:
                                        grid_id = strs[0]
                                        field_id = strs[1]
                                else:
                                    grid_id = 'G0000'
                                    field_id = obj_nm
                                sql = 'select objrank from object_list_all where obj_name=' + "'" + obj_nm + "'" + 'and objsour=' + "'" + objsource + "'"
                                res = sql_get(sql)
                                if res:
                                    objrank = res[0][0]
                                ###
                                if group_id == 'XL002':
                                    name_telescope = 'F60'
                                if group_id == 'XL003':
                                    name_telescope = 'F30'
                                update_pointing_lalalert(trigger,name_telescope,grid_id,field_id,objrank,'Received @ GWAC')
                                ###
                    else:
                        print '\nWrong: Got too many res when send_db_in_beg'
    return

def send_db_in_end(obj):
    infs = get_obj_inf(obj)
    group_id = infs["group_id"]
    objsource = infs['objsour']
    obj_name = infs['obj_name']
    obs_stag = ''
    # i = 0
    # while True:
    #     i += 1
        # ###
        # sql = "SELECT obs_stag, obj_comp_time, unit_id FROM " + pd_log_tab + " WHERE obj_id=" + "'" + obj + "'" + " AND (obs_stag='' or obs_stag='' or obs_stag='') ORDER BY id"
        # res = sql_get(sql)
        # ###
    time.sleep(1.5)
    res = pg_db(pd_log_tab,'select',[['obs_stag','obj_comp_time','unit_id'],{'obj_id':obj}])
    if res:
        if len(res) > 1:
            #print res,len(res)
            res_dic = {}
            for it in res:
                res_dic[it[0]] = it
            if None in res_dic.keys():
                del [res_dic[None]]
            if 'complete' in res_dic.keys():
                obs_stag = 'complete'
                obj_comp_time = res_dic['complete'][1]
                unit_id = res_dic['complete'][2]
            elif 'break' in res_dic.keys():
                obs_stag = 'break'
                obj_comp_time = res_dic['break'][1]
                unit_id = res_dic['break'][2]
            elif 'pass' in res_dic.keys():
                obs_stag = 'pass'
                obj_comp_time = res_dic['pass'][1]
                unit_id = res_dic['pass'][2]
            else:
                pass
            #print obj_comp_time,unit_id
        else:
            obs_stag, obj_comp_time, unit_id = res[0][:]
        #break
    else:
        #if i == 9:
        print '\nWARNING: There is no record in send_db_in_end of %s. Pass to update it.' % obj
            #break
            #print '\nWARNING: There is no record in send_db_in_end of %s.' % obj
            #obj_comp_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time()))
            #pass
    if obs_stag in ['complete','break','pass']:
        if not obj_comp_time:
            obj_comp_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        e_time = datetime.datetime.utcfromtimestamp(time.mktime(time.strptime(obj_comp_time, "%Y-%m-%d %H:%M:%S"))).strftime("%Y-%m-%dT%H:%M:%S")
        endTime = e_time.replace('T',' ')
        if group_id == 'XL001':
            grid_id = 'G0014'
            try:
                strs = obj_name.split('_')
            except:
                field_id = obj_name
            else:
                grid_id = strs[0]
                field_id = strs[1]
            #####
            pg_db(running_list_cur,'update', [{'end_time':e_time},{'obj_id':obj}])
            #####
            update_to_ba_db(objsource, obj, obs_stag, endTime)
            #####
        if group_id in ['XL002','XL003']:
            #####
            pg_db(running_list_cur,'update', [{'end_time':e_time,'unit_id':unit_id},{'obj_id':obj}])
            #####
            try:
                objsour_word = objsource.split('_')
                trigger_type = objsour_word[0]
                version = objsour_word[1]
                trigger = objsour_word[2]
            except:
                pass
            else:
                if trigger_type == 'GW':
                    ###
                    update_to_ba_db(objsource, obj, obs_stag, endTime)
                    ###
                    sql = 'select "Op_Obj_ID" from trigger_obj_field_op_sn where "Trigger_ID"=' + "'" + trigger + "'" + ' and "Serial_num"=' + "'" + version + "'" + ' and "Obj_ID"=' + "'" + obj_name + "'"
                    res = sql_get(sql)
                    if res:
                        objs = []
                        for i in res:
                            objs.append(i[0])
                        objs = list(set(objs))
                        if len(objs) == 1:
                            try:
                                obj_nms = objs[0].split('|')
                            except:
                                obj_nm = res[0]
                                if 'G0' in obj_nm:
                                    try:
                                        strs = obj_nm.split('_')
                                    except:
                                        grid_id = 'G0000'
                                        field_id = obj_nm
                                    else:
                                        grid_id = strs[0]
                                        field_id = strs[1]
                                else:
                                    grid_id = 'G0000'
                                    field_id = obj_nm
                                sql = 'select objrank from object_list_all where obj_name=' + "'" + obj_nm + "'" + 'and objsour=' + "'" + objsource + "'"
                                res = sql_get(sql)
                                if res:
                                    objrank = res[0][0]
                                ###
                                if group_id == 'XL002':
                                    name_telescope = 'F60'
                                if group_id == 'XL003':
                                    name_telescope = 'F30'
                                status = obs_stag + ' @ GWAC'
                                update_pointing_lalalert(trigger,name_telescope,grid_id,field_id,objrank,status)
                                ###
                            else:
                                for obj_nm in obj_nms:
                                    if 'G0' in obj_nm:
                                        try:
                                            strs = obj_nm.split('_')
                                        except:
                                            grid_id = 'G0000'
                                            field_id = obj_nm
                                        else:
                                            grid_id = strs[0]
                                            field_id = strs[1]
                                    else:
                                        grid_id = 'G0000'
                                        field_id = obj_nm
                                    sql = 'select objrank from object_list_all where obj_name=' + "'" + obj_nm + "'" + 'and objsour=' + "'" + objsource + "'"
                                    res = sql_get(sql)
                                    if res:
                                        objrank = res[0][0]
                                    ###
                                    if group_id == 'XL002':
                                        name_telescope = 'F60'
                                    if group_id == 'XL003':
                                        name_telescope = 'F30'
                                    status = obs_stag + ' @ GWAC'
                                    update_pointing_lalalert(trigger,name_telescope,grid_id,field_id,objrank,status)
                                    ###
                        else:
                            print '\nWrong: Got too many res when send_db_in_end'
    return

def check_log_sent(obj, send_beg_time, send_end_time):
    #print send_beg_time, send_end_time
    while True:
        res = pg_db(pd_log_tab,'select',[['obs_stag'],{'obj_id':obj,'obs_stag':'sent'}])
        if res:
            break
        else:
            print "\nWARNING:There is no sent record in check_log_sent of %s." % obj
            time.sleep(1)
            #pass
    infs = get_obj_inf(obj)
    group_id, obj_name = at(infs,'group_id','obj_name')
    if group_id == 'XL001':
        time.sleep(5)
        if check_ser(group_id):
            ser_ip, ser_un, ser_pw = get_ser_config(group_id)[0:3]
            cmd = 'ls /var/log/gtoaes/gtoaes*.log | sort'
            logs = con_ssh(ser_ip, ser_un, ser_pw, cmd)
            if logs:
                logs.reverse()
                sent_mark = 0
                date_mark = 0
                for log in logs:
                    date_mark += 1
                    if date_mark == 3:
                        break
                    log = log.strip()
                    #print log
                    log_date = re.search(r"20\d\d\d\d\d\d", log).group(0)
                    #print log_date
                    log_date = time.strftime("%Y-%m-%d",time.strptime(log_date, "%Y%m%d"))
                    #print log_date
                    cmd = "tac %s | grep -a '.*plan<%s> goes running on .*'" % (log, obj)
                    res = con_ssh(ser_ip, ser_un, ser_pw, cmd)
                    if res:
                        for item in res:
                            item = item.strip()
                            print "\n######"+item
                            log_sent_time = re.search(r"^\d\d:\d\d:\d\d", item).group(0)
                            log_sent_time = "%s %s" % (log_date, log_sent_time)
                            #print log_sent_time, send_beg_time, send_end_time
                            if send_beg_time <= log_sent_time <= send_end_time:
                                log_sent_id = re.search(r'goes running on <001:(.*?)>', item).group(1)
                                sent_mark = 1
                                break
                            else:
                                sent_mark = 0
                        if sent_mark == 1:
                            break
                if sent_mark == 1:
                    #######
                    pg_db(pd_log_tab,'update', [{'obj_sent_time':log_sent_time,'obj_sent_id':log_sent_id},{'obj_id':obj,'obs_stag':'sent'}])
                    #######
                    return [log_sent_id, log_sent_time]
                else:
                    print "\n######WARNING: There is no sent inf of %s in gtoaes !" % obj
                    return 0
            else:
                print "\n######WARNING: There is no gtoaes log of %s when check_sent !" % group_id
                return 0
        else:
            print "\n######WARNING: The gtoaes of %s is Error when check_sent." % group_id
            return 0
    if group_id in ['XL002','XL003']:
        if check_ser(group_id):
            ser_ip, ser_un, ser_pw = get_ser_config(group_id)[0:3]
            cmd = 'ls /tmp/gftservice*.log | sort'
            logs = con_ssh(ser_ip, ser_un, ser_pw, cmd)
            if logs:
                logs.reverse()
                sent_mark = 0
                for log in logs:
                    log = log.strip()
                    cmd = "tac " + log + " | grep 'append object<id.*>.*" + obj_name + "'"
                    res = con_ssh(ser_ip, ser_un, ser_pw, cmd)
                    if res:
                        for item in res:
                            item = item.strip()
                            print "\n######"+item
                            log_sent_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                            #print log_sent_time
                            if send_beg_time <= log_sent_time < send_end_time:
                                log_sent_id = re.search(r'<id = (.*?)>', item).group(1)
                                sent_mark = 1
                                break
                            else:
                                sent_mark = 0
                        if sent_mark == 1:
                            break
                if sent_mark == 1:
                    #######
                    pg_db(pd_log_tab,'update', [{'obj_sent_time':log_sent_time,'obj_sent_id':log_sent_id},{'obj_id':obj,'obs_stag':'sent'}])
                    #######
                    return [log_sent_id, log_sent_time]
                else:
                    print "\n######WARNING: There is no sent inf of %s in gftservice !" % obj
                    return 0
            else:
                print "\n######WARNING: There is no gftservice log of %s when check_sent !" % group_id
                return 0
        else:
            print "\n######WARNING: The gftservice of %s is Error when check_sent." % group_id
            return 0

def check_log_dist(obj,obj_infs):
    time.sleep(1)
    while True:
        res = pg_db(pd_log_tab,'select',[['obj_sent_id','obj_sent_time'],{'obj_id':obj,'obs_stag':'sent'}])
        if res:
            log_sent_id, log_sent_time = res[0][:2]
            print '\nGoing at %s.' % log_sent_time
            break
        else:
            print "\nWARNING:There is no sent record in check_log_dist of %s." % obj
            time.sleep(1)
            #pass
    group_id = obj_infs["group_id"]
    if group_id == 'XL001':
        log_dist_id, log_dist_time = [log_sent_id, log_sent_time][:]
        pg_db(pd_log_tab,'update',[{'obj_dist_id':log_dist_id,'obj_dist_time':log_dist_time,'unit_id':log_dist_id},{'obj_id':obj,'obs_stag':'sent'}])
        return [log_dist_id, log_dist_time]
    if group_id == 'XL002':
        time_limit = (datetime.datetime.now() + datetime.timedelta(hours= -12)).strftime('%Y-%m-%d %H:%M:%S')
        if check_ser(group_id):
            ser_ip, ser_un, ser_pw = get_ser_config(group_id)[0:3]
            cmd = 'ls /tmp/gftservice*.log | sort'
            logs = con_ssh(ser_ip, ser_un, ser_pw, cmd)
            if logs:
                logs.reverse()
                dist_mark = 0
                bk_mark = 0
                for log in logs:
                    log = log.strip()
                    cmd = "cat " + log + " | grep 'get observation plan <id = " + log_sent_id + ">'"
                    res = con_ssh(ser_ip, ser_un, ser_pw, cmd)
                    if res:
                        for item in res:
                            item = item.strip()
                            print "\n"+item
                            log_dist_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                            #print log_sent_time
                            if log_dist_time >= log_sent_time:
                                log_dist_id = re.search(r'<system id = (.*?)>', item).group(1)
                                dist_mark = 1
                                break
                            else:
                                dist_mark = 0
                                if log_dist_time <= time_limit:
                                    bk_mark = 1
                                    break
                        if dist_mark == 1:
                            break
                        if bk_mark == 1:
                            break
                if dist_mark == 1:
                    # if log_dist_id == '1' or log_dist_id == '3':
                    #     unit_id = '001'
                    # if log_dist_id == '2':
                    #     unit_id = '002'
                    if log_dist_id == '1':
                        unit_id = '001'
                    if log_dist_id == '2':
                        unit_id = '002'
                    #######
                    pg_db(pd_log_tab,'update',[{'obj_dist_id':log_dist_id,'obj_dist_time':log_dist_time,'unit_id':unit_id},{'obj_id':obj,'obs_stag':'sent'}])
                    #######
                    return [log_dist_id, log_dist_time]
                else:
                    print "\nThere is no dist inf of %s in gftservice for now." % obj
                    return 0
            else:
                print "\nWARNING: There is no gftservice log of %s when check_dist !" % group_id
                return 0
        else:
            #print "\nWARNING: The gftservice of %s is Error." % group_id
            return 0
    if group_id == 'XL003':
        time_limit = (datetime.datetime.now() + datetime.timedelta(hours= -12)).strftime('%Y-%m-%d %H:%M:%S')
        if check_ser(group_id):
            ser_ip, ser_un, ser_pw = get_ser_config(group_id)[0:3]
            cmd = 'ls /tmp/gftservice*.log | sort'
            logs = con_ssh(ser_ip, ser_un, ser_pw, cmd)
            if logs:
                logs.reverse()
                dist_mark = 0
                bk_mark = 0
                for log in logs:
                    log = log.strip()
                    cmd = "cat " + log #+ " | grep 'get observation plan <id = " + log_sent_id + ">'"
                    res = con_ssh(ser_ip, ser_un, ser_pw, cmd)
                    if res:
                        ii = 0
                        mark_i = 0
                        for item in res:
                            ii += 1
                            item = item.strip()
                            re_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item)
                            if re_time and re_time.group(0) == log_sent_time:
                                mark_i = ii
                            re_null = re.search(r"take NULL object", item)
                            if re_null:
                                if mark_i != 0 and ii > mark_i:
                                    print '\nWARNING: It occured "take NULL object", break it !'
                                    dist_mark = 2
                                    break
                            res_re = re.search(r"^.*?get observation plan <id = %s>" % log_sent_id, item)
                            if res_re:
                                it = res_re.group(0)
                                print '\n' + it
                                log_dist_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", it).group(0)
                                #print log_sent_time
                                if log_dist_time >= log_sent_time:
                                    log_dist_id = re.search(r'<system id = (.*?)>', it).group(1)
                                    dist_mark = 1
                                    break
                                else:
                                    dist_mark = 0
                                    if re_time <= time_limit:
                                        bk_mark = 1
                                        break
                        if dist_mark in [1, 2]:
                            break
                        if bk_mark == 1:
                            break
                if dist_mark == 2:
                    #######
                    time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                    pg_db(pd_log_tab,'update',[{'obj_dist_time':time_now,'obs_stag':'break'},{'obj_id':obj,'obs_stag':'sent'}])
                    time.sleep(1.5)
		    send_db_in_end(obj)
                    client.Send({"obj_id":obj,"obs_stag":'break'},['update','object_list_current','obs_stag'])
                    #######
                    return 0
                elif dist_mark == 1:
                    #######
                    pg_db(pd_log_tab,'update',[{'obj_dist_id':log_dist_id,'obj_dist_time':log_dist_time},{'obj_id':obj,'obs_stag':'sent'}])
                    #######
                    return [log_dist_id, log_dist_time]
                else:
                    print "\nThere is no dist inf of %s in gftservice for now." % obj
                    return 0
            else:
                print "\nWARNING: There is no gftservice log of %s when check_dist !" % group_id
                return 0
        else:
            #print "\nWARNING: The gftservice of %s is Error." % group_id
            return 0

def check_cam_log(obj,obj_infs):
    time.sleep(1)
    while True:
        res = pg_db(pd_log_tab,'select',[['obj_dist_id','obj_dist_time'],{'obj_id':obj,'obs_stag':'sent'}])
        if res:
            log_dist_id, log_dist_time = res[0][:2]
            break
        else:
            #print "\nWARNING:There is no dist record."
            print "\nWARNING:There is no dist record in check_cam_log of %s." % obj
            time.sleep(1)
            #log_dist_id, log_dist_time = ['',''][:2]
            #pass
    group_id = obj_infs['group_id']
    if group_id == 'XL001':
        if check_ser(group_id):
            ser_ip, ser_un, ser_pw = get_ser_config(group_id)[0:3]
            cmd = 'ls /var/log/gtoaes/gtoaes*.log | sort'
            logs = con_ssh(ser_ip, ser_un, ser_pw, cmd)
            if logs:
                logs.reverse()
                com_mark = 0
                date_mark = 0
                for log in logs:
                    date_mark += 1
                    if date_mark == 3:
                        break
                    log = log.strip()
                    log_date = re.search(r"20\d\d\d\d\d\d", log).group(0)
                    log_date = time.strftime("%Y-%m-%d",time.strptime(log_date, "%Y%m%d"))
                    #print log_date
                    cmd = "tac %s | grep -a '.*plan<%s> on .* is over'" % (log, obj)
                    res = con_ssh(ser_ip, ser_un, ser_pw, cmd)
                    if res:
                        for item in res:
                            item = item.strip()
                            log_com_time = re.search(r"^\d\d:\d\d:\d\d", item).group(0)
                            log_com_time = "%s %s" % (log_date, log_com_time)
                            #print log_com_time
                            if log_com_time > log_dist_time:
                                print "\n"+item
                                com_mark = 1
                                break
                            else:
                                com_mark = 0
                        if com_mark == 1:
                            break
                    cmd = "tac %s | grep -a '.*plan<%s> on .* is interrupted for position error'" % (log, obj)
                    res = con_ssh(ser_ip, ser_un, ser_pw, cmd)
                    if res:
                        for item in res:
                            item = item.strip()
                            log_com_time = re.search(r"^\d\d:\d\d:\d\d", item).group(0)
                            log_com_time = "%s %s" % (log_date, log_com_time)
                            #print log_com_time
                            if log_com_time > log_dist_time:
                                print "\n"+item
                                com_mark = 2
                                break
                            else:
                                com_mark = 0
                        if com_mark == 2:
                            break
                if com_mark == 1:
                    expdur, delay, frmcnt = at(obj_infs, 'expdur', 'delay', 'frmcnt')
                    n = int(frmcnt)
                    if n != -1:
                        m1 = float(expdur)
                        m2 = float(delay)
                        # time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()-3600))
                        #time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                        end_time_limit = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.mktime(time.strptime(log_dist_time, "%Y-%m-%d %H:%M:%S")) + (m1+m2)*n - 600 )) ### -10 mins
                        # end_time_e = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.mktime(time.strptime(log_dist_time, "%Y-%m-%d %H:%M:%S")) + (m1+m2)*n + 600 )) ### +10 mins
                        # if time_now > end_time_e:
                        #     print 'complete'
                        # elif end_time_b <= time_now <= end_time_e:
                        #     print 'complete'
                        # else:# time_now < end_time_b
                        #     print 'break'
                        if log_com_time < end_time_limit:
                            #######
                            pg_db(pd_log_tab,'update',[{'obj_comp_time':log_com_time,'obs_stag':'break'},{'obj_id':obj,'obs_stag':'sent'}])
                            #######
                            return [2,log_com_time]
                        else:
                            #######
                            pg_db(pd_log_tab,'update',[{'obj_comp_time':log_com_time,'obs_stag':'complete'},{'obj_id':obj,'obs_stag':'sent'}])
                            #######
                            return [1,log_com_time]
                    else:
                        #######
                        pg_db(pd_log_tab,'update',[{'obj_comp_time':log_com_time,'obs_stag':'complete'},{'obj_id':obj,'obs_stag':'sent'}])
                        #######
                        return [1,log_com_time]
                elif com_mark == 2:
                    #######
                    print "\nThe obj is break for position error !"
                    pg_db(pd_log_tab,'update',[{'obj_comp_time':log_com_time,'obs_stag':'break'},{'obj_id':obj,'obs_stag':'sent'}])
                    #######
                    return [2,log_com_time]
                else:
                    print "\nThere is no complete inf of %s in gtoaes !" % obj
                    return 0
            else:
                print "\nWARNING: There is no gtoaes log of %s when check_com !" % group_id
                return 0
        else:
            #print "\nWARNING: The gtoaes of %s is Error." % group_id
            return 0
    if group_id in ['XL002','XL003']:
        time_limit = (datetime.datetime.now() + datetime.timedelta(hours= -12)).strftime('%Y-%m-%d %H:%M:%S')
        if check_ser(group_id):
            obj_name, filter, frmcnt = at(obj_infs, 'obj_name', 'filter', 'frmcnt')
            date_now = datetime.datetime.utcnow().strftime("%y%m%d")
            cam_ip, cam_un, cam_pw = get_cam_config(log_dist_id)[0:3]
            cmd = 'ps -ef | grep camagent | grep -v grep'
            res = con_ssh(cam_ip, cam_un, cam_pw, cmd)
            if res:
                cmd = 'ls /tmp/camagent*.log | sort'
                logs = con_ssh(cam_ip, cam_un, cam_pw, cmd)
                if logs:
                    logs.reverse()
                    cam_mark = 0
                    bk_mark = 0
                    for log in logs:
                        log = log.strip()
                        cmd = "cat " + log + " | grep 'Image is saved as " + obj_name + ".*_" + filter + "_" + date_now+ "_.*.fit$'"
                        res = con_ssh(cam_ip, cam_un, cam_pw, cmd)
                        if res:
                            count = 0
                            for item in res:
                                item = item.strip()
                                item_com_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                                if item_com_time > log_dist_time:
                                    print '\n'+item
                                    count += 1
                                else:
                                    if item_com_time <= time_limit:
                                        bk_mark = 1
                                        break
                                if count == int(frmcnt):
                                    log_com_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                                    cam_mark = 1
                                    break
                            if cam_mark == 1:
                                break
                            if bk_mark == 1:
                                break
                        if cam_mark == 0:
                                cmd = "tac " + log + ' | grep "20[0-9]*-[0-9]*-[0-9]* [0-9]*:[0-9]*:[0-9]*" | head -1'
                                res = con_ssh(cam_ip, cam_un, cam_pw, cmd)
                                if res:
                                    res = res[0].strip()
                                    re_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", res)
                                    if re_time and re_time.group(0) <= time_limit:
                                        break
                                cmd = "cat " + log + " | grep 'ERROR: Filter input error'"
                                res = con_ssh(cam_ip, cam_un, cam_pw, cmd)
                                if res:
                                    for item in res:
                                        item = item.strip()
                                        item_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                                        if item_time > log_dist_time:
                                            #print '\n' + item
                                            cam_mark = 2
                                            break
                                    if cam_mark == 2:
                                        break
                    # if cam_mark == 0:
                    #     for log in logs:
                    #         log = log.strip()
                    #         cmd = "cat " + log + " | grep 'ERROR: Filter input error'"
                    #         res = con_ssh(cam_ip, cam_un, cam_pw, cmd)
                    #         if res:
                    #             for item in res:
                    #                 item = item.strip()
                    #                 item_time = re.search(r"20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", item).group(0)
                    #                 if item_time > log_dist_time:
                    #                     print '\n' + item
                    #                     cam_mark = 2
                    #                     break
                    #                 else:
                    #                     if item_time <= time_limit:
                    #                         bk_mark = 1
                    #                         break
                    #         if cam_mark == 2:
                    #             break
                    #         if bk_mark == 1:
                    #             break
                    if cam_mark == 2:
                        print "\nWARNING: Filter Error. Please check the system !"
                        log_com_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                        #pg_db(pd_log_tab,'update',[{'obj_comp_time':log_com_time,'obs_stag':'break'},{'obj_id':obj,'obs_stag':'sent'}])
                        return 0#[2, log_com_time]
                    elif cam_mark == 1:
                        #######
                        pg_db(pd_log_tab,'update',[{'obj_comp_time':log_com_time,'obs_stag':'complete'},{'obj_id':obj,'obs_stag':'sent'}])
                        #######
                        return [1,log_com_time]
                    else:
                        print "\nThere is no obs complete inf about %s in camagent log of %s for now!" % (obj_name, log_dist_id)
                        return 0
                else:
                    print "\nWARNING: There is no camagent log of %s!" % log_dist_id
                    return 0
            else:
                print "\nWARNING: The camagent of %s is Error." % log_dist_id
                return 0
        else:
            #print "\nWARNING: The gftservice of %s is Error." % group_id
            return 0

def input():
    global k_in
    k_in = raw_input()

def check_time_window(obj):
    begin_time, end_time = at(get_obj_inf(obj),'begin_time', 'end_time')
    time_now = (datetime.datetime.utcnow() + datetime.timedelta(minutes= 0)).strftime("%Y/%m/%d %H:%M:%S")
    if time_now < end_time:
        if time_now > begin_time:
            return 1
        else:
            return -1
    else:
        return 0

def check_obj_stat_from_data(obj):
    group_id, obj_name, observer, objra, objdec, filter, expdur, frmcnt, end_time = at(get_obj_inf(obj), 'group_id', 'obj_name', 'observer', 'objra', 'objdec', 'filter', 'expdur', 'frmcnt', 'end_time')
    if group_id == 'XL001':
        print '\nPlease clear the sent mark,and restart.'
    cur_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    print "\nCurrent obj : %s %s %s %s %s %s %s." % (obj, obj_name, objra, objdec, filter, str(int(float(expdur))), frmcnt)
    print "\nChecking it in data ..."
    if group_id == "XL002":
        cam_ip, cam_un, cam_pw = get_cam_config('1')[0:3]
        dir = "/home/w60ccd/data/Y2019/%s/" % cur_date
        cmd = "ls " + dir + obj_name + "/" + obj_name + "_"+ filter + "_*_*" + frmcnt + ".fit"
        if con_ssh(cam_ip,cam_un,cam_pw,cmd):
            print "\nThe obj has been observed completely !\n"
            client.Send({"obj_id":obj,"obs_stag":'complete'},['update','object_list_current','obs_stag'])
        else:
            cam_ip, cam_un, cam_pw = get_cam_config('2')[0:3]
            dir = "/home/e60ccd/data/Y2019/%s/" % cur_date
            cmd = "ls " + dir + obj_name + "/" + obj_name + "_"+ filter + "_*_*" + frmcnt + ".fit"
            if con_ssh(cam_ip,cam_un,cam_pw,cmd):
                print "\nThe obj has been observed completely !\n"
                client.Send({"obj_id":obj,"obs_stag":'complete'},['update','object_list_current','obs_stag'])
            else:
                if end_time < time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())):
                    print "\nWARNING: The obs time is over!"
                    client.Send({"obj_id":obj,"obs_stag":'pass'},['update','object_list_current','obs_stag'])
                print "\nGoing..."
    if group_id == "XL003":
        cam_ip, cam_un, cam_pw = get_cam_config('3')[0:3]
        if not observer:
            print "\nWARNING: The observer is not defintion."
        dir = "/home/ccduser/data/Y2019/%s/%s/" % (cur_date, observer)
        cmd = "ls " + dir + obj_name + "/" + obj_name + "*_"+ filter + "_*_*" + frmcnt + ".fit"
        if con_ssh(cam_ip,cam_un,cam_pw,cmd):
            print "\nThe obj has been observed completely !\n"
            client.Send({"obj_id":obj,"obs_stag":'complete'},['update','object_list_current','obs_stag'])
        else:
            if end_time < time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())):
                print "\nWARNING: The obs time is over!"
                client.Send({"obj_id":obj,"obs_stag":'pass'},['update','object_list_current','obs_stag'])
            print "\nGoing..."

def check_obj_stat(obj): ### after check sent
    global k_in, lf
    obj_infs = get_obj_inf(obj)
    check_mark = 0
    #i = 0
    #while True:
    res = pg_db(pd_log_tab,'select',[['obj_sent_time','obj_sent_id'],{'obj_id':obj,'obs_stag':'sent'}])
    if res:
        log_sent_time, log_sent_id = res[0][:]
        #break
    else:
        # if i == 9:
        #     check_mark = 1
        #     print "\nWARNING:There is no sent record in check_obj_stat of %s." % obj
        #     break
        # i += 1
        print "\nWARNING:There is no sent record in check_obj_stat of %s." % obj
        time.sleep(1)
        return 1
    if check_mark == 1:
        check_obj_stat_from_data(obj)
    else:
        group_id, obj_name, objra, objdec, filter, expdur, frmcnt, objrank = at(obj_infs, 'group_id', 'obj_name', 'objra', 'objdec', 'filter', 'expdur', 'frmcnt', 'objrank')
        if group_id == 'XL001':
            ### check the gwac,gtoaes_log
            print "\nCurrent obj : %s %s %s %s %s %s %s." % (obj, log_sent_id, obj_name, objra, objdec, str(int(float(expdur))), frmcnt)
            print "\nChecking it in log ..."
        if group_id in ['XL002','XL003']:
            ### check the gft,log_dist and cam_log
            print "\nCurrent obj : %s %s %s %s %s %s %s." % (obj, obj_name, objra, objdec, filter, str(int(float(expdur))), frmcnt)
            print "\nChecking it in log ..."
        if check_ser(group_id):
            check_log_dist_res = check_log_dist(obj,obj_infs)
            if check_log_dist_res:
                log_dist_id, log_dist_time = check_log_dist_res[:2]
                check_cam_log_res = check_cam_log(obj,obj_infs)
                if check_cam_log_res:
                    log_com_id = check_cam_log_res[0]
                    if group_id == 'XL001':
                        if log_com_id == 1:
                            print "\nThe obj has been observed completely !"
                            send_db_in_end(obj)
                            client.Send({"obj_id":obj,"obs_stag":'complete'},['update','object_list_current','obs_stag'])
                        if log_com_id == 2:
                            print "\nThe obj has been broken !"
                            send_db_in_end(obj)
                            client.Send({"obj_id":obj,"obs_stag":'break'},['update','object_list_current','obs_stag'])
                    if group_id in ['XL002','XL003']:
                        if log_com_id == 1:
                            print "\nThe obj has been observed completely !"
                            send_db_in_end(obj)
                            client.Send({"obj_id":obj,"obs_stag":'complete'},['update','object_list_current','obs_stag'])
                    ###
                    log_com_time = check_cam_log_res[1]
                    log_dist_time_wt = datetime.datetime.utcfromtimestamp(time.mktime(time.strptime(log_dist_time, "%Y-%m-%d %H:%M:%S")))
                    log_com_time_wt = datetime.datetime.utcfromtimestamp(time.mktime(time.strptime(log_com_time, "%Y-%m-%d %H:%M:%S")))
                    lf.write("\n#%s %s %s %s %s_%s %s\n" %(obj_name, objrank, log_dist_time_wt, log_com_time_wt, group_id, log_dist_id,log_com_id))
                    print "\nThe record has been written."
                    ###
                    time.sleep(3)
                    return 1
                else:
                    time.sleep(1.5)
                    if check_time_window(obj) == 0:
                        print "\nWARNING: The observation time is over.\n"
                        log_com_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                        pg_db(pd_log_tab,'update',[{'obj_comp_time':log_com_time,'obs_stag':'pass'},{'obj_id':obj,'obs_stag':'sent'}])
                        send_db_in_end(obj)
                        client.Send({"obj_id":obj,"obs_stag":'pass'},['update','object_list_current','obs_stag'])
                        time.sleep(3)
                        return 1
                    if log_dist_time < time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()-3600)):### wait for 1 hours
                        print "\nWARNING: The obj %s has waited for 60 mins after dist. Please check the system, break current object or not (y/n):" % obj
                        k_in = ''
                        t = threading.Thread(target=input)
                        t.setDaemon(True)
                        t.start()
                        t.join(5)
                        t._Thread__stop()
                        if k_in == 'y':
                            log_com_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                            pg_db(pd_log_tab,'update',[{'obj_comp_time':log_com_time,'obs_stag':'break'},{'obj_id':obj,'obs_stag':'sent'}])
                            send_db_in_end(obj)
                            client.Send({"obj_id":obj,"obs_stag":'break'},['update','object_list_current','obs_stag'])
                            time.sleep(3)
                            return 1
            else:
                time.sleep(1.5)
                if check_time_window(obj) == 0:
                    print "\nWARNING: The observation time is over.\n"
                    log_com_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                    pg_db(pd_log_tab,'update',[{'obj_comp_time':log_com_time,'obs_stag':'pass'},{'obj_id':obj,'obs_stag':'sent'}])
                    send_db_in_end(obj)
                    client.Send({"obj_id":obj,"obs_stag":'pass'},['update','object_list_current','obs_stag'])
                    time.sleep(3)
                    return 1
                if log_sent_time < time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()-1800)):### wait for 30 mins
                    print "\nWARNING: The obj %s has waited for 30 mins after sent. Please check the system, break current object or not (y/n):" % obj
                    if group_id == 'XL003':
                        cam_ip, cam_un, cam_pw = get_cam_config('3')[0:3]
                        cmd = 'autostop ; autostart '
                        con_ssh(cam_ip,cam_un,cam_pw,cmd)
                    k_in = ''
                    t = threading.Thread(target=input)
                    t.setDaemon(True)
                    t.start()
                    t.join(5)
                    t._Thread__stop()
                    if k_in == 'y':
                        log_com_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                        pg_db(pd_log_tab,'update',[{'obj_comp_time':log_com_time,'obs_stag':'break'},{'obj_id':obj,'obs_stag':'sent'}])
                        send_db_in_end(obj)
                        client.Send({"obj_id":obj,"obs_stag":'break'},['update','object_list_current','obs_stag'])
                        time.sleep(3)
                        return 1
        else:
            # if group_id == 'XL001':
            #     print "\nWARNING: The gtoaes of %s is Error." % group_id
            # if group_id in ['XL002','XL003']:
            #     print "\nWARNING: The gftservice of %s is Error." % group_id
            time.sleep(1.5)

def get_msg():
    print ''
    client.Recv()
    data = client.recv_data
    if data["content"]:
        msg = data["content"]
        return msg
    else:
        return 0

def get_grp_id(obj):
    sql = "SELECT group_id FROM object_list_all WHERE obj_id='" + obj + "'"
    res = sql_get(sql)
    if res:
        return res[0][0]

def get_pri(obj):
    sql = "SELECT priority FROM object_list_all WHERE obj_id='" + obj + "'"
    res = sql_get(sql)
    if res:
        return res[0][0]
######
def get_sent_indb(group_id):
    objs = []
    sql = "SELECT object_list_current.obj_id FROM object_list_current, object_list_all WHERE object_list_current.obj_id=object_list_all.obj_id AND object_list_current.obs_stag='sent' and object_list_all.group_id='"+ group_id +"' AND object_list_current.mode='observation' ORDER BY object_list_current.id"
    res = sql_get(sql)
    if res:
        for i in res:
            objs.append(i[0])
    return objs

def get_new_indb(group_id):
    objs = []
    sql = "SELECT object_list_current.obj_id FROM object_list_current, object_list_all WHERE object_list_current.obj_id=object_list_all.obj_id AND object_list_current.obs_stag='scheduled' and object_list_all.group_id='"+ group_id +"' AND object_list_current.mode='observation' ORDER BY object_list_current.id"
    res = sql_get(sql)
    if res:
        for i in res:
            objs.append(i[0])
    return objs

def get_com_indb(group_id):
    objs = []
    sql = "SELECT object_list_current.obj_id FROM object_list_current, object_list_all WHERE object_list_current.obj_id=object_list_all.obj_id AND object_list_current.obs_stag in ('complete','break','pass') and object_list_all.group_id='"+ group_id +"' AND object_list_current.mode='observation' ORDER BY object_list_current.id"
    res = sql_get(sql)
    if res:
        for i in res:
            objs.append(i[0])
    return objs
#####

# def get_sent_indb():
#     gwac_sent_objs = []
#     f60_sent_objs = []
#     f30_sent_objs = []
#     sql = "SELECT obj_id FROM object_list_current WHERE obs_stag='sent' ORDER BY id"
#     res = sql_get(sql)
#     if res:
#         for i in res:
#             group_id = get_grp_id(i[0])
#             if group_id == "XL001":
#                 gwac_sent_objs.append(i[0])
#             if group_id == "XL002":
#                 f60_sent_objs.append(i[0])
#             if group_id == "XL003":
#                 f30_sent_objs.append(i[0])
#     return [gwac_sent_objs,f60_sent_objs,f30_sent_objs]

# def get_new_indb():
#     gwac_new_objs = []
#     f60_new_objs = []
#     f30_new_objs = []
#     sql = "SELECT obj_id FROM object_list_current WHERE obs_stag='scheduled' AND mode='observation' ORDER BY id"
#     res = sql_get(sql)
#     if res:
#         for i in res:
#             group_id = get_grp_id(i[0])
#             if group_id == "XL001":
#                 gwac_new_objs.append(i[0])
#             if group_id == "XL002":
#                 f60_new_objs.append(i[0])
#             if group_id == "XL003":
#                 f30_new_objs.append(i[0])
#     return [gwac_new_objs,f60_new_objs,f30_new_objs]

# def get_com_indb():
#     gwac_com_objs = []
#     f60_com_objs = []
#     f30_com_objs = []
#     sql = "SELECT obj_id FROM object_list_current WHERE obs_stag in ('complete', 'pass', 'break') AND mode='observation'"
#     res = sql_get(sql)
#     if res:
#         for i in res:
#             group_id = get_grp_id(i[0])
#             if group_id == "XL001":
#                 gwac_com_objs.append(i[0])
#             if group_id == "XL002":
#                 f60_com_objs.append(i[0])
#             if group_id == "XL003":
#                 f30_com_objs.append(i[0])
#     return [gwac_com_objs,f60_com_objs,f30_com_objs]

def init_f30():
    cam_ip, cam_un, cam_pw = get_cam_config('3')[0:3]
    img_types = ['flat','bias','dark']
    cur_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    last_date = (datetime.datetime.utcnow() + datetime.timedelta(days= -1)).strftime("%Y-%m-%d")
    cmd = 'ls /home/data_proc/Y2019/%s' % cur_date
    res = con_ssh(cam_ip, cam_un, cam_pw, cmd)
    if not res:
        cmd = 'mkdir -p /home/data_proc/Y2019/%s' % cur_date
        con_ssh(cam_ip, cam_un, cam_pw, cmd)
    #cmd = 'cp -r /home/ccduser/data/Y2019/%s/* /home/data_proc/Y2019/%s' % (cur_date, cur_date)
    #con_ssh(cam_ip, cam_un, cam_pw, cmd)
    for it in img_types:
        img_path_last = "/home/ccduser/data/Y2019/%s/%s" % (last_date, it)
        img_to_path_last = '/home/data_proc/Y2019/%s' % last_date
        cmd = 'cp -r %s %s' % (img_path_last, img_to_path_last)
        con_ssh(cam_ip, cam_un, cam_pw, cmd)
        img_path_cur = "/home/ccduser/data/Y2019/%s/%s" % (cur_date, it)
        img_to_path_cur = '/home/data_proc/Y2019/%s' % cur_date
        cmd = 'cp -r %s %s' % (img_path_cur, img_to_path_cur)
        con_ssh(cam_ip, cam_un, cam_pw, cmd)
    return

def check_F30_sync():
    cam_ip, cam_un, cam_pw = get_cam_config('3')[0:3]
    while True:
        cmd = 'ps -ef | grep sync_remote | grep -v grep '
        res = con_ssh(cam_ip, cam_un, cam_pw, cmd)
        if not res:
            print '\n\n\033[0;31m########## Please restart the sync_remote_by_inotify.sh of F30 in /home/ccduser/sync !\033[0m\n\n'
        time.sleep(30)


def inits():
    date_c = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    #pd_log_tab, running_list_cur
    sql = 'insert into '+pd_log_tab.replace('current','history')+' (obj_id, obj_name, obj_sent_time, obj_dist_time, obj_comp_time, group_id,unit_id,obj_sent_id,obj_dist_id,obs_stag,date_cur,priority) SELECT obj_id, obj_name, obj_sent_time, obj_dist_time, obj_comp_time, group_id,unit_id,obj_sent_id,obj_dist_id,obs_stag,date_cur,priority FROM '+pd_log_tab+' WHERE date_cur<'+"'"+ date_c +"'"
    sql_get(sql,0)
    sql = 'delete from '+pd_log_tab+' WHERE date_cur<'+"'"+ date_c +"'"
    sql_get(sql,0)
    ##
    sql = 'insert into '+running_list_cur.replace('current','history')+' (cmd,op_sn,op_time,op_type,obj_id,obj_name,observer,objra,objdec,objepoch,objerror,group_id,unit_id,obstype,obs_stra,grid_id,field_id,ra,dec,epoch,imgtype,filter,expdur,delay,frmcnt,priority,begin_time,end_time,run_name,pair_id,note,mode) SELECT cmd,op_sn,op_time,op_type,obj_id,obj_name,observer,objra,objdec,objepoch,objerror,group_id,unit_id,obstype,obs_stra,grid_id,field_id,ra,dec,epoch,imgtype,filter,expdur,delay,frmcnt,priority,begin_time,end_time,run_name,pair_id,note,mode FROM '+running_list_cur+' WHERE op_time<'+"'"+ date_c +"'"
    sql_get(sql,0)
    sql = 'delete from '+running_list_cur+' WHERE op_time<'+"'"+ date_c +"'"
    sql_get(sql,0)
    return

def check_sent():
    global lf
    cur_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    if not os.path.exists('obslogs'):
        os.system('mkdir obslogs')
    lf = open("obslogs/log_%s.txt" % cur_date, "a+")
    while True:
        time.sleep(1)
        #os.system('clear')
        ###
        cur_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        time_h = time.strftime("%H:%M", time.localtime(time.time()))
        if time_h == '7:30':
            #init_f30()
            inits()
            time.sleep(65)
        if time_h == '8:05':
            lf.close()
            lf = open("obslogs/log_%s.txt" % cur_date, "a+")
            lf.write("# obj_name objrank begin_time end_time group_id\n")
            time.sleep(65)
        ###
        #sent_objs = get_sent_indb()
        gwac_sent_objs = get_sent_indb('XL001')
        f60_sent_objs = get_sent_indb('XL002')
        f30_sent_objs = get_sent_indb('XL003')
        if gwac_sent_objs:
            print "\n\n\nCurrent objs of GWAC: " + ','.join(gwac_sent_objs)
            for obj in gwac_sent_objs:
                check_obj_stat(obj)
                print '\n'
        if f60_sent_objs:
            print "\n\n\nCurrent objs of F60: " + ','.join(f60_sent_objs)
            for obj in f60_sent_objs:
                check_obj_stat(obj)
                print '\n'
        if f30_sent_objs:
            print "\n\n\nCurrent objs of F30: " + ','.join(f30_sent_objs)
            for obj in f30_sent_objs:
                check_obj_stat(obj)
                print '\n'

def get_free_teles_from_log(type):
    gwac_init = ['002','003','004']
    gwac_frees = []
    f60_frees = []
    f30_frees = []
    cur_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    ###group=XL001
    if type == 'XL001':
        if check_ser(type):
            ser_ip, ser_un, ser_pw = get_ser_config(type)[0:3]
            cmd = 'ls /var/log/gtoaes/gtoaes*.log | sort'
            logs = con_ssh(ser_ip, ser_un, ser_pw, cmd)
            if logs:
                logs.reverse()
                auto_mark = 0
                for id in gwac_init:
                    date_mark = 0
                    auto_mark1 = 0
                    auto_mark2 = 0
                    for log in logs:
                        date_mark += 1
                        if date_mark == 3:
                            break
                        log = log.strip()
                        #print(log)
                        log_date = re.search(r"20\d\d\d\d\d\d", log)
                        if log_date:
                            log_date = log_date.group(0)
                        #print log_date
                        log_date = time.strftime("%Y-%m-%d",time.strptime(log_date, "%Y%m%d"))
                        cmd = "tac %s | grep -a 'Mount<001:%s> is .*line' | head -1" % (log, id)
                        res = con_ssh(ser_ip, ser_un, ser_pw, cmd)
                        if res:
                            #print res
                            res = ''.join(res).strip()
                            res_time = re.search(r"^\d\d:\d\d:\d\d", res)
                            if res_time:
                                res_time = res_time.group(0)
                                res_time = "%s %s" % (log_date, res_time)
                                #print res_time
                                res_time_utc = datetime.datetime.utcfromtimestamp(time.mktime(time.strptime(res_time, "%Y-%m-%d %H:%M:%S"))).strftime("%Y-%m-%d")
                                if res_time_utc == cur_date:
                                    res_stat = re.search(r"is (.*?)-line", res).group(1)
                                    if res_stat == 'on':
                                        gwac_frees.append(id)
                                        auto_mark1 += 1
                                        auto_mark += 1
                                    else:
                                        auto_mark2 = -1
                        #print auto_mark1,auto_mark2
                        if auto_mark1 != 0 or auto_mark2 != 0:
                            break
                if auto_mark >= 1:
                    return gwac_frees
                else:
                    return 0
            else:
                return 0
        else:
            return 0

    ###group=XL002
    if type == 'XL002':
        if check_ser(type):
            ser_ip, ser_un, ser_pw = get_ser_config(type)[0:3]
            cmd = 'ls /tmp/gftservice*.log'
            logs = con_ssh(ser_ip, ser_un, ser_pw, cmd)
            if logs:
                logs.reverse()
                #print(logs)
                auto_mark = 0
                for log in logs:
                    log = log.strip()
                    for uid in ['1','2']:
                        cmd = "tac " + log + " | grep '<system id = %s> .* automatic .*' | head -1" % uid
                        #print(cmd)
                        res = con_ssh(ser_ip, ser_un, ser_pw, cmd)
                        #print(res)
                        if res:
                            res = ''.join(res).strip()
                            res_time = re.search(r"^20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", res).group(0)
                            res_time_utc = datetime.datetime.utcfromtimestamp(time.mktime(time.strptime(res_time, "%Y-%m-%d %H:%M:%S"))).strftime("%Y-%m-%d")
                            if res_time_utc == cur_date:
                                res_stat = re.search(r"<system id = 1> (.*?) automatic", res).group(1)
                                if res_stat == 'enter':
                                    if check_cam(uid):
                                        if uid == '1':
                                            f60_frees.append('001')
                                        if uid == '2':
                                            f60_frees.append('002')
                                        auto_mark = 1
                                    else:
                                        auto_mark = -1
                                else:
                                    auto_mark = -1
                            else:
                                auto_mark = -1
                    if auto_mark != 0:
                        break
                if auto_mark == 1:
                    return f60_frees
                else:
                    return 0
            else:
                return 0
        else:
            return 0
    ###group=XL003
    if type == 'XL003':
        if check_ser(type):
            ser_ip, ser_un, ser_pw = get_ser_config(type)[0:3]
            cmd = 'ls /tmp/gftservice*.log'
            logs = con_ssh(ser_ip, ser_un, ser_pw, cmd)
            if logs:
                logs.reverse()
                auto_mark = 0
                for log in logs:
                    log = log.strip()
                    cmd = "tac " + log + " | grep '<system id = 3> enter .* mode' | head -1"
                    res = con_ssh(ser_ip, ser_un, ser_pw, cmd)
                    if res:
                        res = ''.join(res).strip()
                        res_time = re.search(r"^20\d\d-\d\d-\d\d \d\d:\d\d:\d\d", res).group(0)
                        res_time_utc = datetime.datetime.utcfromtimestamp(time.mktime(time.strptime(res_time, "%Y-%m-%d %H:%M:%S"))).strftime("%Y-%m-%d")
                        if res_time_utc == cur_date:
                            res_stat = re.search(r"<system id = 3> enter (.*?) mode", res).group(1)
                            if res_stat == 'automatic':
                                if check_cam('3'):
                                    f30_frees.append('001')
                                    auto_mark = 1
                                else:
                                    auto_mark = -1
                            else:
                                if check_cam('3'):
                                    print('\nWARNING: The system of XL003 is in idle mode now.')
                                auto_mark = -1
                        else:
                            auto_mark = -1
                    if auto_mark != 0:
                        break
                if auto_mark == 1:
                    return f30_frees
                else:
                    return 0
            else:
                return 0
        else:
            return 0

# def get_uesd_teles_from_db(type):
#     gwac_used = []
#     f60_used = []
#     f30_used = []
#     gwac_pris = []
#     f60_pris = []
#     f30_pris = []
#     date_cur = datetime.datetime.utcnow().strftime("%Y-%m-%d")
#     if type == 'XL001':
#         res = pg_db(pd_log_tab,'select',[['unit_id','priority'],{'obs_stag':'sent','date_cur':date_cur,'group_id':type}])
#         if res:
#             for it in res:
#                 gwac_used.append(it[0])
#                 gwac_pris.append(it[1])
#             return [gwac_used,gwac_pris]
#         else:
#             return 0
#     if type == 'XL002':
#         res = pg_db(pd_log_tab,'select',[['unit_id','priority'],{'obs_stag':'sent','date_cur':date_cur,'group_id':type}])
#         if res:
#             for it in res:
#                 f60_used.append(it[0])
#                 f60_pris.append(it[1])
#             return  [f60_used,f60_pris]
#         else:
#             return 0
#     if type == 'XL003':
#         res = pg_db(pd_log_tab,'select',[['unit_id','priority'],{'obs_stag':'sent','date_cur':date_cur,'group_id':type}])
#         if res:
#             for it in res:
#                 f30_used.append(it[0])
#                 f30_pris.append(it[1])
#             return  [f30_used,f30_pris]
#         else:
#             return 0

# def sync():
#     sql = "SELECT obj_id FROM object_list_current WHERE obs_stag in ('complete', 'pass', 'break') AND mode='observation'"
#     res = sql_get(sql)
#     if res:
#         objs1 = res
#         #print objs1
#         sql = "SELECT obj_id FROM "+pd_log_tab+" WHERE obs_stag='sent'"
#         res = sql_get(sql)
#         if res:
#             objs2 = res
#             #print objs2
#             for obj in objs2:
#                 if obj in objs1:
#                     obj = obj[0]
#                     sql = "SELECT obs_stag FROM object_list_current WHERE obj_id='"+obj+"'"
#                     res = sql_get(sql)
#                     if res:
#                         obs_stag = res[0][0]
#                         #print obs_stag
#                         sql = "UPDATE "+pd_log_tab+" SET obs_stag='"+obs_stag+"' WHERE obj_id='"+obj+"'"
#                         sql_get(sql,0)

def get_uesd_teles_from_db(group_id):
    used_units = []
    unit_pris = []
    date_cur = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    res = pg_db('pd_log_current','select',[['unit_id','priority'],{'obs_stag':'sent','date_cur':date_cur,'group_id':group_id}])
    if res:
        res.sort(key=operator.itemgetter(1))
        res_dic_set_lis = sorted(dict(res).items(), key=operator.itemgetter(1))
        for k, v in res_dic_set_lis:
            used_units.append(k)
            unit_pris.append(v)
        return [unit_pris,used_units]
    else:
        return 0

def pre_units(group_id,obj_infs):
    used_sts = get_uesd_teles_from_db(group_id)
    #print used_sts
    if used_sts:
         pri_list, used_units= used_sts[:]
    else:
        pri_list = ['0']
        used_units = []
    if group_id == 'XL001':
        if 'GW' in str(obj_infs['objsour']):
            initunits = gwac_init #+ ['003']
        else:
            initunits = gwac_init #['002','004']
    if group_id == 'XL002':
        initunits = f60_init
    if group_id == 'XL003':
        initunits = f30_init
    if not used_units:
        unused_units = initunits[:]
    else:
        unused_units = list(set(initunits) - set(used_units))
    for_new_units = unused_units + used_units
    return [initunits, unused_units, for_new_units, pri_list]

def check_new_main(group_id):
    global send_objs
    mark_wait = 0
    type_tel = {'XL001':'GWAC', 'XL002':'F60', 'XL003':'F30'}

    new_objs = get_new_indb(group_id)
    com_objs = get_com_indb(group_id)

    lf.write('\n##### %s_new_objs(%s): ' % (type_tel[group_id] ,str(len(new_objs))) + ','.join(new_objs))
    print '\n##### %s_new_objs: %s ' % (type_tel[group_id] ,str(len(new_objs)))# + ','.join(new_objs)
    lf.write('\n##### %s_com_objs(%s): ' % (type_tel[group_id] ,str(len(com_objs))) + ','.join(com_objs))
    print '\n##### %s_com_objs: %s ' % (type_tel[group_id] ,str(len(com_objs)))# + ','.join(com_objs)
    if new_objs:
        lf.write('\n##### %s : Case 1' % type_tel[group_id])
        for obj in new_objs:
            if obj not in send_objs:
                send_objs.append(obj)
                obj_infs = get_obj_inf(obj)
                initunits, unused_units, for_new_units, pri_list = pre_units(group_id,obj_infs)[:]
                print '\n'
                print pre_units(group_id,obj_infs)
                obj_pri = obj_infs['priority']
                if int(obj_pri) > int(pri_list[0]):
                    units = for_new_units
                    if pri_list[0] == '0':
                        print '\n##### Common, %s ' % obj_pri
                    else:
                        print '\n##### Higher Priority, %s ' % obj_pri
                else:
                    units = unused_units
                    print '\n##### Common, %s ' % obj_pri
                if units:
                    #print units
                    unit_id = obj_infs['unit_id']
                    if len(unit_id) > 3:
                        unit_id = units[0]
                    if unit_id in units:
                        time_res = check_time_window(obj)
                        if time_res == 1:
                            lf.write('\n###### The obj %s of %s: Sending.' % (obj,type_tel[group_id]))
                            print '\n###### The obj %s of %s: Sending.' % (obj,type_tel[group_id])
                            send_beg_time, send_end_time = send_cmd(obj, unit_id)[:]
                            for it in range(5):
                                if check_log_sent(obj, send_beg_time, send_end_time):
                                    check_log_sent_res = 1
                                    break
                                else:
                                    check_log_sent_res = 0
                                    time.sleep(it)
                            if check_log_sent_res == 1:
                                lf.write("\n##### The obj %s of %s: Send ok." % (obj,type_tel[group_id]))
                                print "\n##### The obj %s of %s: Send ok." % (obj,type_tel[group_id])
                                client.Send({"obj_id":obj,"obs_stag":'sent'},['update','object_list_current','obs_stag'])
                                send_db_in_beg(obj)
                                time.sleep(5)
                            else:
                                print "\n##### %s WARNING: The obj of %s: Send Wrong." % (obj, type_tel[group_id])
                                lf.write("\n##### The obj %s of %s: Send Wrong." % (obj, type_tel[group_id]))
                                pg_db(pd_log_tab,'update',[{'obs_stag':'resend'},{'obj_id':obj,'obs_stag':'sent'}])
                        elif time_res == 0:
                            client.Send({"obj_id":obj,"obs_stag":'pass'},['update','object_list_current','obs_stag'])
                            print "\n##### The obj %s of %s: Pass Ok." % (obj, type_tel[group_id])
                            lf.write("\n##### The obj %s of %s: Pass Ok." % (obj, type_tel[group_id]))
                            send_objs.remove(obj)
                            break
                        else:
                            print "\n##### %s WARNING: The obj of %s: Need wait." % (obj, type_tel[group_id])
                            lf.write("\n##### The obj %s of %s: Need Wait." % (obj, type_tel[group_id]))
                            mark_wait = 1
                            send_objs.remove(obj)
                            break
                else:
                    print "\n##### %s WARNING: There is no free units of %s, don't need to send." % (type_tel[group_id],obj)
                    lf.write("\n##### There is no free units of %s when send %s, don't need to send." % (type_tel[group_id],obj))
                    send_objs.remove(obj)
                    break
                send_objs.remove(obj)
            else:
                print '\n##### %s : Case Wrong. ' % type_tel[group_id]
                break
    else:
        lf.write('\n##### %s : Case 2' % type_tel[group_id])
    return mark_wait

def sync():
    while True:
        time.sleep(0.5)
        mark = 0
        objs1 = []
        objs2 = []
        pd_log_tab = 'pd_log_current'
        sql = "SELECT obj_id FROM object_list_current WHERE obs_stag in ('sent') AND mode='observation'"
        res = sql_get(sql)
        if res:
            objs1 = res
            #print objs1
        sql = "SELECT obj_id FROM "+pd_log_tab+" WHERE obs_stag in ('complete', 'pass', 'break')"
        res = sql_get(sql)
        if res:
            objs2 = res
            #print objs2
        for obj in objs2:
            if obj in objs1:
                mark = 1
        if mark == 0:
            break
    return

def check_new():
    while True:
        msg = get_msg()
        if msg:
            time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            print '\n###### Get news (%s)' % time_now
            lf.write("\n###### Get news (%s) ######" % time_now)
            #sync()
            mark_wait = 0
            for group_id in ['XL001','XL002','XL003']:
                print '\n\n\n'
                k = check_new_main(group_id)
                mark_wait += k
            if mark_wait:
                time.sleep(45)
                xclient.Send("Hello World",['insert'])
            print '\n###### Ready to get news.'
            lf.write('\n###### Ready to get news ######')
            time.sleep(1)
    return


if __name__ == "__main__":
    print '\nInit DB...'
    inits()
    time.sleep(1)
    print "\nBegin..."
    thread_floup = threading.Thread(target=pd_followup)
    thread_floup.setDaemon(True)
    thread_floup.start()
    time.sleep(1)
    thread_inif30 = threading.Thread(target=check_F30_sync)
    thread_inif30.setDaemon(True)
    thread_inif30.start()
    time.sleep(1)
    thread_main = threading.Thread(target=check_sent)
    thread_main.setDaemon(True)
    thread_main.start()
    time.sleep(1)
    check_new()
