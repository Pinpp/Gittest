#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time,datetime

def trigger_type_of_time(trigger_time):
    ### Suppose input using utc time.
    time_now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    trigger_time_t = time.mktime(time.strptime(trigger_time, '%Y-%m-%d %H:%M:%S'))
    time_now_t = time.mktime(time.strptime(time_now, '%Y-%m-%d %H:%M:%S'))
    if (time_now_t - trigger_time_t) <= 180: ### 3 mins
        mark4 = 10
    elif 180 < (time_now_t - trigger_time_t) <= 1800: ### 30 mins
        mark4 = 20
    elif (time_now_t - trigger_time_t) > 1800:
        mark4 = 30
    else:
        print 'Time delay check Failed'
        mark4 = 0
    return mark4

def trigger_type_of_init(alert_message_type):
    if alert_message_type == 'Initial':
        mark2 = 1000
    elif alert_message_type == 'Update':
        mark2 = 2000
    else:
        print "Something is Wrong."
        mark2 = 0
    return mark2
