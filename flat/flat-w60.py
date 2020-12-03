#!/usr/bin/env python
# -*- coding: utf-8 -*-
#---pinp---

import os,sys,time,datetime,platform
import pyfits
import numpy as np

#清屏命令
#print platform.system()
if platform.system() == 'Windows':
    cmd = 'cls'
else:
    cmd = 'clear'

#sys.argv.append('')
if not sys.argv[1:]:
    mark = 0
else:
    fil = sys.argv[1]
    expdur = sys.argv[2]
    mark = 1

date_cur = datetime.datetime.utcnow().strftime("%Y-%m-%d")
path = '/home/w60ccd/data/Y2019/%s' % date_cur

if mark == 0:
    #fils = ['U','B','I','V','R','Lum']
    fils = ['B','I','V','R','Lum']
    #fils = ['null']
    expdur = '3'
    for fil in fils:
        cmd1 = 'light 1 2 test %s %s 1' % (fil, expdur)
        while True:
            #print cmd1
            os.system(cmd1)
            len1 = 0
            while True:
                cmd2 = 'ls %s/test/test_%s_*_*.fit 2>/dev/null' % (path,fil)
                #print cmd2
                res = os.popen(cmd2)
                fits = res.readlines()
                #print fits
                len2 = len(fits)
                if len1 == 0 and len2 == 1:
                    fit = fits[-1]
                    break
                if len1 != 0 and len2 > len1:
                    fit = fits[-1]
                    break
                len1 = len2
                time.sleep(1)
            # time.sleep(int(expdur)+2.5)
            # cmd2 = 'ls %s/test_%s_*_*.fit' % (path,fil)
            # print cmd2
            # res = os.popen(cmd2)
            # fits = res.readlines()
            # print fits
            # #fit = fits[-1]
            #print fit
            print '\nGet the fit: '+fit
            while True:
                try: 
                    hdulist = pyfits.open(fit)
                    a = hdulist[0].data
                    x = int(np.median(a[500,:]))
                    y = int(a[1500,:])
                except:
                    #print 'Wrong.'
                    pass
                else:
                    print x,y,'\n'
                    break
            if abs(x-y) < 300:
                break
            elif abs(x-y) > 1000:
                time.sleep(30)
            else:
                time.sleep(5)
        time.sleep(float(expdur)+1)
        cmd3 = 'flat 1 2 %s %s 5' % (fil, expdur)
        #print cmd3
        os.system(cmd3)
        len1 = 0
        while True:
            cmd2 = 'ls %s/flat/flat_%s_*_*.fit 2>/dev/null' % (path,fil)
            #print cmd2
            res = os.popen(cmd2)
            fits = res.readlines()
            len3_e = len(fits)
            if len3_e:
                print fits[-1]
            # if len3 == 5:
            #     break
            if len1 == 0:
                len3_b = len3_e
                len1 += 1
            if (len3_e - len3_b) == 5:
                break
            time.sleep(float(expdur)+1)
