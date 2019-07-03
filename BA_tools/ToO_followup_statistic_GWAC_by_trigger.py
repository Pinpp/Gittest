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
from func_gwac_too_image_status_query import func_gwac_too_image_status_query
from first_and_last_image import first_and_last_image
# load the adapter
import psycopg2
# load the psycopg extras module
import psycopg2.extras
try:
  sys.path.append("./coor_convert/")
  from dd2dms import dd2dms
  from dd2hms import dd2hms
except:
  print "please install sidereal code "

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

    location = 'xinglong'
    if location == 'xinglong':
        configuration_file = 'configuration_xl.dat'
    elif location == 'beijing':
        configuration_file = 'configuration_bj.dat'

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

    CurrentTable = "object_running_list_current"
    CurrentTable_all = "object_list_all"
    query = ("SELECT " + CurrentTable + ".op_time," + CurrentTable + ".grid_id," + CurrentTable + ".field_id,"  + CurrentTable + ".ra," + CurrentTable + ".dec"\
        " from " + CurrentTable + "," + CurrentTable_all + \
        " where ( " + CurrentTable+".group_id='XL001'"\
        " and " + CurrentTable_all + ".objsour like \'%" + trigger_id + "%\' " + \
        " and ( " + CurrentTable + ".obstype = \'toa\' or " + CurrentTable + ".obstype = \'tom\' ) " + \
        " and " + CurrentTable + ".obj_id = " + CurrentTable_all + ".obj_id"\
        ") ")

    db = Yunwei_DBConnect(location)
    cursor = db.cursor()

    cursor.execute(query)
    rows = cursor.fetchall()

    Grid_ID = []
    Field_ID = []
    Pointing_RA = []
    Pointing_DEC = []
    if rows > 1:
        for row in rows:
            if utc_datetime_begin_str_T <= row[0] <= utc_datetime_end_str_T:
                Grid_ID.append(row[1])
                Field_ID.append(row[2])
                Pointing_RA.append(row[3])
                Pointing_DEC.append(row[4])
    else:
        print 'no too record returns'
    Yunwei_DBClose(db)

    u,indices = np.unique(Field_ID, return_index=True)
    # print utc_time_str,u,indices

    if len(u) > 0:
        for nin in indices:
            # utc_datetime_str = "2018-04-28 00:00:00"
            # utc_datetime = datetime.datetime.strptime(utc_datetime_str, '%Y-%m-%d %H:%M:%S')
            # utc_datetime_begin_str = "2018-04-28 17:57:00"
            # utc_datetime_begin = datetime.datetime.strptime(utc_datetime_begin_str, '%Y-%m-%d %H:%M:%S')
            # utc_datetime_end_str = "2018-04-28 18:00:00"
            # utc_datetime_end = datetime.datetime.strptime(utc_datetime_end_str, '%Y-%m-%d %H:%M:%S')
            # Pointing_RA[nin] = 233.87
            # Pointing_DEC[nin] = 74.5
            data = func_gwac_too_image_status_query(configuration_file,utc_datetime,utc_datetime_begin,utc_datetime_end,float(Pointing_RA[nin]),float(Pointing_DEC[nin]))
            if len(data[0]) >= 1:
                # for n_data in range(len(data[0])):
                    # print("%s %s %s %s %s %6.2f %6.2f %s %s %8.4f %8.4f" % \
                    #     (Object_ID[nin],Trigger_ID[nin],ObjRA[nin],ObjDEC[nin],data[0][n_data],data[1][n_data],data[2][n_data],data[6][n_data],data[7][n_data],data[4][n_data],data[5][n_data]))           
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
                    outline = '%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s' % (Grid_ID[nin],Field_ID[nin],trigger_id,Pointing_RA[nin],Pointing_DEC[nin],B_UT , E_UT, M_coor_ra_deg, M_coor_dec_deg, M_coor_ra, M_coor_dec, Image_coor_ra_deg, Image_coor_dec_deg, Image_coor_ra, Image_coor_dec, CCD_ID, CCD_TYPE)
                    print outline
                    p += 1

                # file_root = 'obs_map_'
                # plot_file_name = file_root + Trigger_ID[nin] + '_' + utc_datetime_str + '.png'    
                # plot_file = path + plot_file_name          
                # obs_field_plot(statistic_file,plot_file,Trigger_ID[nin],ObjRA[nin],ObjDEC[nin],ObjError[nin])





