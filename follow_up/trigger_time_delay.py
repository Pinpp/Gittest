#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os,sys,time,datetime

def trigger_type_of_time(trigger_time):
    time_now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    trigger_time_t = time.mktime(time.strptime(trigger_time, '%Y-%m-%d %H:%M:%S'))
    time_now_t = time.mktime(time.strptime(time_now, '%Y-%m-%d %H:%M:%S'))
    if (time_now_t - trigger_time_t) <= 180:
        mark4 = 10
    elif (time_now_t - trigger_time_t) <= 1800:
        mark4 = 20
    else:
        mark4 = 30
    return mark4

def trigger_type_of_init(alert_message_type):
    if alert_message_type == 'Initial':
        mark2 = 1000
    elif alert_message_type == 'Update':
        mark2 = 2000
    else:
        mark2 = 0
        print "Something is Wrong."
    return mark2
