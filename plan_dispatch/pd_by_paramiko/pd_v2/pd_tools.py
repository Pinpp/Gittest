#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import json, psycopg2, paramiko

###
warn_bs = {}
###

def load_params():
    json_file = './pd_params.json'
    with open(json_file) as read_file:
        pd_params = json.load(read_file)
    return pd_params

def con_db():
    pd_params = load_params()
    try:
        db = psycopg2.connect(**pd_params['db'])
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
            if n == 0:
                cur.execute(sql)
                db.commit()
            else:
                cur.execute(sql)
                rows = cur.fetchall()
                return rows
        except psycopg2.Error as e:
            #print "\nWARNING: Wrong with operating the db, %s " % str(e).strip()
            warn_bs['db'] = "WARNING: Wrong with operating the db, " + str(e).strip()
            return False
        finally:
            cur.close()
            db.close()
    else:
        #print "\nWARNING: Connection to the db is Error."
        warn_bs['db'] = "WARNING: Connection to the db is Error."

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
            cond_keys = args[1].keys()
            conds = []
            if len(args) > 2:
                cond_more = args[2]
            else:
                cond_more = ''
            for key in cond_keys:
                conds.append("='".join([key,args[1][key]]))
            cond = "' AND ".join(conds)
            sql = "SELECT " + rows + " FROM " + table + " WHERE " + cond + "' " + cond_more 
            res = sql_act(sql)
            return res

def con_ssh(ip, username, passwd, cmd):
    global warn_bs
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
    try:
        ssh.connect(hostname=ip, port=22, username=username, password=passwd, timeout=60)
    except:
        #print "\nWARNING: Connection of ssh is wrong!"
        warn_bs['ssh'] = 'WARNING: Connection to %s by ssh is wrong!' % ip
    else:
        stdin, stdout, stderr = ssh.exec_command(cmd,get_pty=True)
        out = stdout.readlines()
        ssh.close()
        return out