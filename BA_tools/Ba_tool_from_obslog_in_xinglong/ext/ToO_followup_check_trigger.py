"""

   Pubsub envelope subscriber   
 
   Author: Xuhui Han
  
"""

import sys
import os
import datetime
import time
import json
#import mysql.connector
import numpy as np
# from func_gwac_too_image_status_query import func_gwac_too_image_status_query
# from first_and_last_image import first_and_last_image
# load the adapter
import psycopg2
# load the psycopg extras module
import psycopg2.extras

#__________________________________
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

if __name__ == '__main__':
#    print sys.argv  
    if not sys.argv[1:]:
        sys.argv += [ "S190512at", "2019-05-13T15:00:00", "12800", "3600"]

    trigger_id = sys.argv[1]
    search_time_str = sys.argv[2]
    check_window_pre = float(sys.argv[3])
    check_window_post = float(sys.argv[4])

    utc_time_str = search_time_str
    utc_datetime = datetime.datetime.strptime(utc_time_str, '%Y-%m-%dT%H:%M:%S')
    check_window_pre = float(check_window_pre)/3600.0
    check_window_post = float(check_window_post)/3600.0
    utc_datetime_begin = utc_datetime - datetime.timedelta(hours=check_window_pre)
    utc_datetime_end = utc_datetime + datetime.timedelta(hours=check_window_post)
    utc_datetime_begin_str = datetime.datetime.strftime(utc_datetime_begin, '%Y-%m-%d %H:%M:%S')
    utc_datetime_end_str = datetime.datetime.strftime(utc_datetime_end, '%Y-%m-%d %H:%M:%S')
    utc_datetime_begin_str_T = datetime.datetime.strftime(utc_datetime_begin, '%Y-%m-%dT%H:%M:%S')
    utc_datetime_end_str_T = datetime.datetime.strftime(utc_datetime_end, '%Y-%m-%dT%H:%M:%S')

    CurrentTable = "object_list_all"
    query = ("SELECT obj_name,objsour,group_id,objra,objdec,obs_type"\
        " from " + CurrentTable + \
        " where ( " + \
        " ( objsour like \'%" + trigger_id + "%\' ) " + \
        # " ( objsour like \'%S19051%\' ) " + \
        " and "  \
        "( group_id = \'XL001\' ) " + \
        # " and ( obs_type = \'ToA\' or obs_type = \'ToM\' ) " + \
        ") ")
    print(query)
    location = 'beijing'

    db = Yunwei_DBConnect(location)
    cursor = db.cursor()

    cursor.execute(query)
    rows = cursor.fetchall()
    if len(rows) > 1:
        for row in rows:
            print(row[0],row[1])
    else:
        print('no too record returns')
    Yunwei_DBClose(db)