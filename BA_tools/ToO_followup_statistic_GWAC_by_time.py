"""

   Pubsub envelope subscriber   
 
   Author: Xuhui Han
  
"""

import sys
import os
import datetime
import time
import json
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
  print("please install sidereal code ")

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
        sys.argv += [ "2019-06-30T12:04:36", "2019-06-30T18:04:36", "./"]
    
    utc_datetime_begin_str_T = sys.argv[1]
    utc_datetime_end_str_T = sys.argv[2]
    filepath = sys.argv[3]

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

    if utc_datetime_begin >= DB_switch_datetime:
        CurrentTable = "object_running_list_current"
    else:
        CurrentTable = "object_running_list_history"

    CurrentTable_all = "object_list_all"
    query = ("SELECT " + CurrentTable + ".op_time," + CurrentTable + ".unit_id," + CurrentTable + ".grid_id," + CurrentTable + ".field_id,"  + CurrentTable + ".ra," + CurrentTable + ".dec"\
        " from " + CurrentTable + "," + CurrentTable_all + \
        " where ( " + CurrentTable_all+".group_id='XL001'"\
        # " and " + CurrentTable_all + ".objsour like \'%" + trigger_id + "%\' " + \
        # " and ( " + CurrentTable_all + ".obs_type = \'toa\' or " + CurrentTable_all + ".obs_type = \'tom\' ) " + \
        " and " + CurrentTable + ".obj_id = " + CurrentTable_all + ".obj_id"\
        " and " + CurrentTable + ".op_time > \'" + utc_datetime_begin_str_T + "\'"\
        " and " + CurrentTable + ".op_time < \'" + utc_datetime_end_str_T + "\'"\
        ") ")
    print(query)

    db = Yunwei_DBConnect(location)
    cursor = db.cursor()

    cursor.execute(query)
    rows = cursor.fetchall()
    # print(rows)
    op_time = []
    unit_id = []
    Grid_ID = []
    Field_ID = []
    Pointing_RA = []
    Pointing_DEC = []
    if len(rows) > 1:
        for row in rows:
        # if utc_datetime_begin_str_T <= row[0] <= utc_datetime_end_str_T:
            op_time.append(row[0])
            unit_id.append(row[1])
            Grid_ID.append(row[2])
            Field_ID.append(row[3])
            Pointing_RA.append(row[4])
            Pointing_DEC.append(row[5])
    else:
        print('no record returns')
    Yunwei_DBClose(db)

    u,indices = np.unique(Field_ID, return_index=True)

    if len(u) > 0:
        for nin in indices:
            print(op_time[nin],unit_id[nin],Grid_ID[nin],Field_ID[nin],Pointing_RA[nin],Pointing_DEC[nin])
        print("Grid_ID,Field_ID,trigger_id,Pointing_RA,Pointing_DEC,B_UT , E_UT, M_coor_ra_deg, M_coor_dec_deg, M_coor_ra, M_coor_dec, Image_coor_ra_deg, Image_coor_dec_deg, Image_coor_ra, Image_coor_dec, CCD_ID, CCD_TYPE")  
        for nin in indices:
            # utc_datetime_str = "2018-04-28 00:00:00"
            # utc_datetime = datetime.datetime.strptime(utc_datetime_str, '%Y-%m-%d %H:%M:%S')
            # utc_datetime_begin_str = "2018-04-28 17:57:00"
            # utc_datetime_begin = datetime.datetime.strptime(utc_datetime_begin_str, '%Y-%m-%d %H:%M:%S')
            # utc_datetime_end_str = "2018-04-28 18:00:00"
            # utc_datetime_end = datetime.datetime.strptime(utc_datetime_end_str, '%Y-%m-%d %H:%M:%S')
            # Pointing_RA[nin] = 233.87
            # Pointing_DEC[nin] = 74.5
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
                    outline = '%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s' % (Grid_ID[nin],Field_ID[nin],Pointing_RA[nin],Pointing_DEC[nin],B_UT , E_UT, M_coor_ra_deg, M_coor_dec_deg, M_coor_ra, M_coor_dec, Image_coor_ra_deg, Image_coor_dec_deg, Image_coor_ra, Image_coor_dec, CCD_ID, CCD_TYPE)
                    print(outline)
                    p += 1

                # file_root = 'obs_map_'
                # plot_file_name = file_root + Trigger_ID[nin] + '_' + utc_datetime_str + '.png'    
                # plot_file = path + plot_file_name          
                # obs_field_plot(statistic_file,plot_file,Trigger_ID[nin],ObjRA[nin],ObjDEC[nin],ObjError[nin])
