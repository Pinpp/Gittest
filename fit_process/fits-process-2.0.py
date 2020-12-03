#!/usr/bin/env python
# -*- coding: utf-8 -*-
#---pinp---

import os
import sys
import time
import pyfits
import threading
import operator
import platform
import numpy as np
import matplotlib.pyplot as plt


#清屏命令
#print platform.system()
if platform.system() == 'Windows':
    cmd = 'cls'
else:
    cmd = 'clear'

#程序运行方式
sys.argv.append('')
fsf = sys.argv[1]##fsf = fitsfile

if (len(fsf) >= 4) and (os.path.exists(fsf) == True):
    os.system(cmd)
    print u'\033[0;36m******************************\n输入文件：\033[0m'
    print fsf
else:
    os.system(cmd)
    fsf0 = 'G17-Flat.fits'
    while True:
        print '\033[0;36m******************************\033[0m'
        print u'''\033[0;36m请输入要处理的平场图名称:
(如 G17-Flat.fits)\033[0m'''
        fsf = raw_input('')
        if len(fsf) == 0:
            fsf = fsf0
            print fsf
        else:
            pass 
        if os.path.exists(fsf) == False:
            print u'\n<平场文件不存在或输入错误>'
            time.sleep(1.2)
            os.system(cmd)
            pass
        else:
            break
print '\nDone \n'
print '\033[0;36m******************************\033[0m'

##fits文件读取
print u'\n文件读取 ...'

hdulist = pyfits.open(fsf)
#print hdulist.info()
A0 = hdulist[0].data
b0 = np.median(A0)
C0 = A0/b0

print '\nDone +\n'
print '\033[0;36m******************************\033[0m'

##将二维数组转化为三维坐标形式
print u'\n转化并提取数据 ...\n'

A = C0

m = len(A[:,0])#m = len(A) ##行数
n = len(A[0,:])#n = len(A[0])##列数

Z = []
D_X = []
D_Y = []
D = []
C = []
def Choose():
    D_X.append(x)
    D_Y.append(y)
    D.append(z)
    C.append(color)
    return

#转化为三维坐标并提取数据
i = 21
while i <= n-80:
    j = 1
    while j <= m:
        x = i
        y = j
        z = A[j-1,i-1]

        if z <= 0.1:
            color = 'k'

            Choose()

        elif z <= 0.5:
            color = 'b'
        
            Choose()
        
        elif z < 0.7:
            color = 'c'

            Choose()
        
        elif z <= 0.9:
            color = 'y'

            #Choose()
       
        elif z < 1.1:
            color = 'gray'
    
        elif z < 1.3:
            color = 'm'

            #Choose()
        
        else:
            color = 'r'

            Choose()
        
        Z.append(z)

        j = j+1
    
    sys.stdout.write('Done...%.1f%%\r'%((i-20)*100.0/(n-100)))
    sys.stdout.flush()

    i = i+1

Min = min(Z)
Max = max(Z)

print '\n\nDone + +\n'
print '\033[0;36m******************************\033[0m'


print u'\n绘图 ...'

##多线程
def plot_show():

    f0 = plt.figure(u'热度图',facecolor='w')
    ax0 = f0.add_subplot(111)           
    cmap = plt.cm.get_cmap('YlGnBu_r',1000)#('nipy_spectral', 1000000)
    map = ax0.imshow(A, interpolation="nearest", cmap=cmap,aspect='auto', vmin=-0.5,vmax=2)
    cb = plt.colorbar(mappable=map, cax=None, ax=None,shrink=0.5)
    cb.set_label('ratio')

    plt.gca().invert_yaxis()


    f1 = plt.figure(u'异常点二维展示&')

    ax1 = plt.subplot(111)
    ax1.scatter(D_X,D_Y,color = C,marker = '.')
    ax1.set_xlabel('X')
    ax1.set_xlim(0,n)
    ax1.set_ylabel('Y')
    ax1.set_ylim(0,m)

    print '\nShow...'
    plt.show()
    #print '\nPlot Done\n'
    return None

#线程任务
t = threading.Thread(target=plot_show)
t.start()
t.join(150)

print '\nCoutinue ...\n'

low = Min+0.1
high = 0.0
while True:
    print u'''输入坏像素判定条件（low high）：
Min = %s    Max = %s'''%(Min,Max)

    kw = raw_input('')

    if len(kw) == 0:
        break
    else:
        try:
            low = float(kw.split()[0])
            high = float(kw.split()[1])
        except :#IndexError:
            print u'\n<输入错误，请重输>\n'
        #except ValueError:
            #print u'\n<输入错误，请重输>\n'
        else:
            if ((low < Max) and (high == 0.0))or((low < Max) and (high > Min)):
                break
            else:
                print u'\n<输入错误，请重输>\n'

print u'\n坏像素提取 ...\n'
B_X = []
B_Y = []
B_Z = []
def Choose_bad(k):
    B_X.append(D_X[k])
    B_Y.append(D_Y[k])
    B_Z.append(D[k])
    return

num = len(D)
k = 0
while k < num:

    if D[k] <= low:
        Choose_bad(k)

    if D[k] >= high:
        if high != 0.0:
            Choose_bad(k)
    
    sys.stdout.write('Done...%.2f%%\r'%((k+1.0)*100/num))
    sys.stdout.flush()

    k += 1

print '\nDone + + +\n'
print '\033[0;36m******************************\033[0m'

##打印坏点
print u'\n输出文件 ...'

#坏像点
print u'\n坏像素'
if os.path.exists(fsf[0:fsf.index('.')]+'_'+'Badpixels.dat') == False:
    xfile = open(fsf[0:fsf.index('.')]+'_'+'Badpixels.dat','a')
else:
    os.remove(fsf[0:fsf.index('.')]+'_'+'Badpixels.dat')
    xfile = open(fsf[0:fsf.index('.')]+'_'+'Badpixels.dat','a')

num2 = len(B_Z)
for i in range(0,num2):
    xfile.write(str(B_X[i])+' '+str(B_Y[i])+' '+str(B_Z[i])+'\n')
xfile.close()

#坏像列和坏像行
if os.path.exists(fsf[0:fsf.index('.')]+'_'+'Bad-rows&lines.dat') == False:
    yfile = open(fsf[0:fsf.index('.')]+'_'+'Bad-rows&lines.dat','a')
else:
    os.remove(fsf[0:fsf.index('.')]+'_'+'Bad-rows&lines.dat')
    yfile = open(fsf[0:fsf.index('.')]+'_'+'Bad-rows&lines.dat','a')

print u'\n坏像列'
yfile.write('Bad Rows:\n')

s_x = set(B_X)
bd_rows = []
for item1 in s_x:
    if B_X.count(item1) >= 8:#坏像列判断
        row = (item1,B_X.count(item1))
        bd_rows.append(row)

bd_rows.sort(key=operator.itemgetter(0))#重排序

for nb in range(0,len(bd_rows)):
    row_x = list(bd_rows[nb])[0]
    row_y = list(bd_rows[nb])[1]
    yfile.write(str(row_x)+' '+str(row_y)+'\n')

#坏像行
print u'\n坏像行'
yfile.write('\nBad Lines:\n')

s_y = set(B_Y)
for item2 in s_y:
    if B_Y.count(item2) >= 8:#坏像行判断
        yfile.write(str(item2)+' '+str(B_Y.count(item2))+'\n')

yfile.close()
print '\nDone + + + +\n'
print '\033[0;36m******************************\033[0m'