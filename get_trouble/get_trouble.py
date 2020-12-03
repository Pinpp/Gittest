#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import re, os, sys, time, datetime, threading
import psycopg2, csv
import pandas as pd
# reload(sys) 
# sys.setdefaultencoding('utf-8')


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
        except psycopg2.Error, e:
            print "\nWARNING: Wrong with operating the db, %s " % str(e).strip()
            return False
        finally:
            db.close()
    else:
        print "\nWARNING: Connection to the db is Error."



if __name__ == "__main__":
    unit = '30cm'
    datacsv = open('trouble-%s.csv' % unit,'w+')
    cw = csv.writer(datacsv,dialect = ("excel"))
    sql = "select troubletxt.device, troubletxt.txt, troubletxt.date_in from troubletxt, device, devicegroup where troubletxt.device=device.name and device.affiliation=devicegroup.name and (devicegroup.affiliation='"+ unit +"' or devicegroup.name='"+ unit +"') order by troubletxt.date_in"
    res = sql_get(sql)
    #print res
    if res:
        for i in res:
            # for j in i:
            #     print j
            #     print j.decode('utf-8')
            cw.writerow(i)
        print 'Done'
    else:
        print 'None'
    datacsv.close()
    
    df =  pd.read_csv('trouble-%s.csv' % unit)
    df.to_excel('trouble-%s.xlsx' % unit, header=False, index=False)
