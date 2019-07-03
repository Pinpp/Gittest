#!/usr/bin/env python
# -*- coding: utf-8 -*-
#---pinp---

import sys
import json
import pymysql,psycopg2
import datetime
import time

#__________________________________
def CMM_DBConnect(location):
    "DB connection with the parameters in the static file db_param.json. The file is not available on the git because it contains databse informations. Contact Damien Turpin or Cyril Lachaud for Help if needed."
    with open("db_param.json", "r") as read_file:
        data = json.load(read_file)
    if location == 'beijing':
        CMM_user = data["CMM_user_bj"]
    elif location == 'xinglong':
        CMM_user = data["CMM_user_xl"]
    db = pymysql.connect(data["CMM_host"],CMM_user,data["CMM_password"],data["CMM_db"] )
    return db

#__________________________________
def CMM_DBClose(db):
    "Close the connection to the DB"
    db.close()

def Yunwei_DBConnect(location):
    "DB connection with the parameters in the static file db_param.json. The file is not available on the git because it contains databse informations. Contact Damien Turpin or Cyril Lachaud for Help if needed."
    with open("db_param.json", "r") as read_file:
        data = json.load(read_file)
    if location == 'beijing':
        yunwei_host = data["yunwei_host_bj"]
    elif location == 'xinglong':
        yunwei_host = data["yunwei_host_xl"]
    db=psycopg2.connect("host=" + yunwei_host + " port= " + data["yunwei_port"] + \
        " dbname='" + data["yunwei_db"] + "' user='" + data["yunwei_user"] + "' password='"  + data["yunwei_password"] + "'")
    return db

#__________________________________
def Yunwei_DBClose(db):
    "Close the connection to the DB"
    db.close()

def upload_pointing_gwacalert(ID_external_trigger,name_telescope,ID_grid,ID_field_arr,RA_pointing_arr,dec_pointing_arr,grade_pointing_arr,pointing_status):
    location = 'xinglong'
    db = CMM_DBConnect(location)
    query = "INSERT INTO pointing_gwacalert "+ \
    "( ID_pointing_gwacalert, " + \
    "ID_external_trigger, " + \
    "name_telescope, " + \
    "ID_grid, " + \
    "ID_field, " + \
    "RA_pointing, " + \
    "dec_pointing," + \
    "grade_pointing," + \
    "pointing_status )" + \
    "VALUES ( DEFAULT ,"+ \
    "'" + str(ID_external_trigger) + "'," + \
    "'" + str(name_telescope) + "'," + \
    "'" + str(ID_grid) + "'," + \
    "'" + str(RA_pointing_arr) + "'" + \
    "'" + str(dec_pointing_arr) + "'" + \
    "'" + str(grade_pointing_arr) + "'" + \
    "'" + str(pointing_status) + "'" + \
    ")"
    try:
        cursor = db.cursor()  
        cursor.execute(query)
        db.commit()
        cursor.close()
    except Exception as e:
        print 'Error %s' % e
    return  