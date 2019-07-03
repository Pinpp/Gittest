#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import os
import time
import codecs
import threading

import warnings
warnings.filterwarnings("ignore")


##基础设定
num = 0
music = 'start /b Music.mp3'

#起止时间
start_time = '19:00'
end_time = '29:00'

##可用设备
nM = '0/12'
nG = '3/4'
nT6 = '1/2'
nT3 = '0/1'


##获取当前时间
def get_curtime():
        global curTime,curtime
        curTime = time.strftime('%Y-%m-%d %H:%M:%S')
        curtime = time.strftime("%H:%M",time.localtime())
        return curtime,curtime


##报警模块
def alertfunc():

        get_curtime()

        print '\n请在slack中更新观测情况'
        print '\n例：\n\nBeijingTime(UTC+8) %s: obs=running, dome=open, weather=clear, gwac=%s, 60cm=%s, 30cm=%s\n' % (curTime,nG,nT6,nT3)
        print '\n当前已提醒: %d 次' % i
        print '\n\n\n备用：\nBeijingTime(UTC+8) %s: obs=stop/running/, dome=open/closed, weather=cloudy/clear/snow/rain, gwac=%s, 60cm=%s, 30cm=%s' % (curTime,nG,nT6,nT3)        
        return


##初始化模块

#结束时间合理性判断
key = 0
def sensabl():
        global E_h,E_m,key
        E_h,E_m = end_time.split(':')
      
        if int(E_h) < 8:
                E_h = str(int(E_h) + 24)
                #print E_h
                #time.sleep(1.5)

        if int(H0) < int(E_h):
                key = 1
        if ((int(H0) == int(E_h)) and (int(M0)) < int(E_m)):
                key = 1
        if ((int(H0) == int(E_h)) and (int(M0) >= int(E_m))):
                key = 0
                print '\n警告：时间设置不当\n'
                time.sleep(1.5)
        if int(H0) > int(E_h):
                key = 0
                print '\n警告：时间设置不当\n'
                time.sleep(1.5)
        return

#多线程
Lock = threading.Lock() 
def key_input():
        global ki
        Lock.acquire()
        ki = '0'
        k_input = raw_input(':')
        if len(k_input) != 0:
                ki = k_input
                Lock.release()
        else:
                Lock.release()
        return

#初始化函数
def initfunc():
        global start_time,H0,M0,end_time,nM,nG,nT6,nT3,E_h,E_m,time_list,Time_list,ki,key
        
        get_curtime()
        
        if num == 0:#参数赋值
                start_time = curtime
                H0,M0 = start_time.split(':')
                if int(H0) < 8:
                        H0 = str(int(H0)+24)
                if not os.path.exists('slackFiles'):
                        os.makedirs('slackFiles')
                try:
                        setting_file = open('slackFiles/slack_reminder-setting.dat','r')
                except IOError:
                        pass
                else:
                        setting_values = setting_file.readline()
                        end_time,nM,nG,nT6,nT3 = setting_values.split(',')[2:7]
                        setting_file.close()
        else:#参数读取
                try:
                        setting_file = open('slackFiles/slack_reminder-setting.dat','r')
                except IOError:
                        pass
                else:
                        setting_values = setting_file.readline()
                        H0,M0,end_time,nM,nG,nT6,nT3 = setting_values.split(',')[0:7]
                        setting_file.close()

        #设置
        M0 = str(int(M0)+1)
        ki = '0'        
        while True:
                os.system('cls')
                print '''\n是否需要更改设置（将每隔两小时提醒一次）

        1：初次提醒的小时数      （当前：H0 = %s ）

        2：每次提醒的分钟数      （当前：M0 = %s ）

        3：结束观测的时间        （当前：End_time = %s）

        4：Mini-gwac 设备数  （当前：Mini-gwac = %s）

        5：GWAC 设备数       （当前：GWAC = %s）

        6：60CM 设备数       （当前：60cm = %s）

        7：30CM 设备数       （当前：30cm = %s）

输入相应的数字标记以进行更改，或Enter键退出更改
''' %(H0,M0,end_time,nM,nG,nT6,nT3)

                t = threading.Thread(target=key_input)
                t.setDaemon(True)
                t.start()
                t.join(8)

                if ki == '0':
                        sensabl()
                        if key == 1:
                                break

                if ki in ['1','2','3','4','5','6','7']:
                        while True:
                                print '\n输入更改值' 
                                kii = raw_input(':')
                                if (kii == 'b') or (len(kii) == 0):
                                        ki = '0'
                                        break
                                
                                if ki == '1':
                                        if len(kii) in [1,2]:
                                                H0 = kii
                                                sensabl()
                                                ki = '0'
                                                break
                                        else:
                                              pass
                                if ki == '2':
                                        if len(kii) in [1,2]:
                                                M0 = kii
                                                sensabl()
                                                ki = '0'
                                                break
                                        else:
                                              pass
                                if ki == '3':
                                        if len(kii) in [3,4,5]:
                                                end_time = kii
                                                sensabl()
                                                ki = '0'
                                                break
                                        else:
                                              pass
                                if ki == '4':
                                        if len(kii) in [4,5]:
                                                nM = kii
                                                break
                                                ki = '0'
                                        else:
                                              pass
                                if ki == '5':
                                        if len(kii) in [3]:
                                                nG = kii
                                                ki = '0'
                                                break
                                        else:
                                              pass
                                if ki == '6':
                                        if len(kii) in [3]:
                                                nT6 = kii
                                                ki = '0'
                                                break
                                        else:
                                                pass
                                if ki == '7':
                                        if len(kii) in [3]:
                                                nT3 = kii
                                                ki = '0'
                                                break
                                        else:
                                                pass


        #时间列表
        time_list = range(int(H0),int(E_h)+1,2)                
        Time_list = time_list[:]

        os.system('cls')
        print Time_list,':',M0,"    ",end_time
        time.sleep(1.5)

        #保存设置
        setting_list = [H0,',',M0,',',end_time,',',nM,',',nG,',',nT6,',',nT3]
        setting_file = open('slackFiles/slack_reminder-setting.dat','w')
        for setting in setting_list:
                setting_file.write(setting)
        setting_file.close()
        return


##附加小程序
'startmark'
"""
#!/usr/bin/env python
#coding=utf-8
# pinp

from __future__ import unicode_literals
import os
import time
import threading
import msvcrt


k = 0
i = 0


## 条件判断函数

def iffunc(i):
        global k

        if (i in [5,10,20]):
                print '\n这程序耐玩\n'
        if (i >= 20):
                print '\n再搞就不好玩了\n'
        if (i >= 25):
                print '\n再玩就崩了\n'
        if (i == 27):
                k = 3
        print i
        
        return


## 多线程函数

def func_back():
        global k1
        print '\n\nEnter键返回?'
        k3 = raw_input(':')
        k1 = 'y'
        return


while True:
        
        ##读取设置
        read_setting_file = open('slackFiles/slack_reminder-setting.dat','r')
        setting_values = read_setting_file.readline()
        nM,nG,nT6,nT3 = setting_values.split(',')[3:7]

        ##读取当前时间
        p1 = '\n请在slack中更新观测情况:'
        H,M= time.strftime('%H %M',time.localtime()).split()
        TIME = time.strftime('%y-%m-%d %H:%M:%S')
        Time = time.strftime('%H:%M')

        if int(H) < 8:
                H = str(int(H)+24)
        
        ##程序
        if k == 0:
                os.system('cls')
#                print ('''\n是否需要临时在slack中更新观测信息?
#\n（输入y确认）\n''')
#                k1 = raw_input(':')
                k1 = 'y'
                
        if k1 in ['y','e','0']:
                if k1 == 'y':
                        os.system('cls')
                        print '''\n是否需要临时在slack中更新观测信息?
\n输入标记以更新：\n
        1:观测重新开始
        
        2:观测异常结束
        
        3:中微子暴接收
        
        4:引力波暴接收
        
        5:未找到 grid file
        
        6:引力波暴开始观测
        
        7:引力波暴观测状态

'''

                        k2 = raw_input(':')
                        if k2 in ['1','2','3','4','5','6','7','b','e','0']:
                                if k2 == str(1):
                                        print '\n',H,':',M
                                        print p1
                                        print '\n\nBeijingTime(UTC+8) %s: \
obs=start, dome=open, weather=clear, minigwac=%s, gwac=%s, 60cm=%s, 30cm=%s\n' % (TIME,nM,nG,nT6,nT3)
                                        k = 1

                                if k2 == str(2):
                                        print '\n',H,':',M
                                        print p1
                                        print '\n\nBeijingTime(UTC+8) %s:\
obs=stop, dome=closed, weather=cloudy/snow/rain, minigwac=%s, gwac=%s, 60cm=%s, 30cm=%s\n' % (TIME,nM,nG,nT6,nT3)
                                        k = 1

                                if k2 == str(3):
                                        print '\n',H,':',M
                                        print p1
                                        print '\n\nalert received, target: ok, when: %s, instrument:60cm/30cm\n' % Time
                                        k = 1
                                
                                if k2 == str(4):
                                        print '\n',H,':',M
                                        print p1
                                        print '\n\nBeijingTime(UTC+8) %s \
received: GW= ID XXXXXXX, curobsstatus=running/stop\n' % TIME
                                        k = 1

                                if k2 == str(5):
                                        print '\n',H,':',M
                                        print p1
                                        print '\n\n%s,alert received but there is no grid files in the website\n' % Time
                                        k = 1

                                if k2 == str(6):
                                        print '\n',H,':',M
                                        print p1
                                        print '\n\nBeijingTime(UTC+8) %s \
Observation: GW= ID XXXXXXX, curobsstatus=running pointingTrigger=start\n' % TIME
                                        k = 1

                                if k2 == str(7):
                                        print '\n',H,':',M
                                        print p1
                                        print '\n\nBeijingTime(UTC+8) %s \
Observation: GW= ID XXXXXXX, curobsstatus=running/stop pointingTrigger=keep/stop\n' % TIME
                                        k = 1

                                if k2 == 'b':
                                        k = 0
                                        pass
                                if k2 == 'e':
                                        k1 = 'e'
                                if k2 == '0':#####归零
                                        i = 0

                        else:
                                k = 2

                if k1 == 'e':
                        k = 3
                if k1 == '0':#####归零
                        i = 0

        else:
                i += 1
                print '\n搞事情\n'
                iffunc(i)
                time.sleep(1)

        if k == 1:
                t = threading.Thread(target=func_back)
                t.start()
                t.join(15)
                k1 = 'y'
                
        if k == 2:
                i += 1 
                print '\n搞事情\n'
                iffunc(i)
                time.sleep(1)
                k1 = 'y'
                
        if k == 3:
                break

"""
'stopmark'

##提醒程序自运行
while True:

        ##初始化操作
        initfunc()


        #小程序自复制操作
        if num == 0:
                original_file = codecs.open(__file__,'r','utf-8')
                lines = original_file.readlines()
                startmark_num = lines.index("'startmark'\n") + 2
                stopmark_num = lines.index("'stopmark'\n") - 2

                try:
                        object_file = codecs.open('slackFiles/slack_reminder-multi.py','r','utf-8')
                except IOError:
                        object_file = codecs.open('slackFiles/slack_reminder-multi.py','w','utf-8')
                        for code in lines[startmark_num:stopmark_num]:
                                object_file.write(code)
                        object_file = codecs.open('slackFiles/slack_reminder-multi.py','r','utf-8')
                else:
                        pass  

                while True:
                        object_file = codecs.open('slackFiles/slack_reminder-multi.py','r','utf-8')
                        Lines = object_file.readlines()
                        j = cmp(lines[startmark_num:stopmark_num],Lines)
                        if j != 0:
                                object_file = codecs.open('slackFiles/slack_reminder-multi.py','w','utf-8')
                                for code in lines[startmark_num:stopmark_num]:
                                        object_file.write(code)
                        else:
                                break

                original_file.close()
                object_file.close()

                time.sleep(1)
                os.system('start python slackFiles/slack_reminder-multi.py')


        ##自动提醒程序运行部分
        print 'Runnig'
        i = 1
        while True:

                get_curtime()

                H = curtime.split(':')[0]
                M = curtime.split(':')[1]

                if int(H) < 8:
                        H = str(int(H)+24)

                if i == 1:
                        Time_list = time_list[:]

                if int(H) in Time_list and int(M) == int(M0):

                        loc_H = Time_list.index(int(H))
                        #print loc0
                        
                        if loc_H != 0:
                                del Time_list[:(loc_H-1)]
                        #else:
                        #        del time_list[0]

                        os.system('cls')
                        print Time_list,':',M0,'    ',end_time
                        print '\n',H+':'+M
                        
                        alertfunc()
                        
                        i += 1
                                
                        os.system(music)

                if int(H)==int(E_h) and int(M)==int(E_m):

                        os.system('cls')
                        get_curtime()
                        print '\n',H,':',M
                        print '\n\n请在slack中更新观测情况'
                        print '\n\nBeijingTime(UTC+8) %s: obs=stop, dome=close, weather=clear,oa=away \n\nOr\n\nBeijingTime(UTC+8) %s: obs=stop, dome=close, weather=clear,oa=away' % (curTime,curTime)

                        os.system(music)

                        i = 1

                try:
                        time.sleep(58)
                except KeyboardInterrupt:
                        os.system('del slackFiles\slack_reminder-multi.py')
                        #os.system('del slackFiles\slack_reminder-setting.dat')
                        break
                        #try:
                        #        break
                        #except TypeError:
                        #        os.system('exit')
                else:
                        pass
        
        num += 1
        print '\n\n\nRuning Times:%d' %num
        time.sleep(300)

