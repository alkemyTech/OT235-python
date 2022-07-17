# -*- coding: utf-8 -*-
import xml.etree.ElementTree as ET
from operator import itemgetter
import sys
import re

mapped=dict()

tag='row'

def reduce_c_args(function, iterable, n, rev, flag_orden, initializer=None):
    it = iter(iterable)
    if initializer is None:
        value = next(it)
    else:
        value = initializer
    for element in it:
        value = function(value, element,n,rev,flag_orden)
    return value

def reducer_menores(p, c, top,rev,fo):
    n=[]
    if isinstance(p[0],list):
        n=p
    else:
        n.append(p)
    n.append(c)
    if len(n)>top:
        N=top
    else:
        N=len(n)
    return sorted(n,key=itemgetter(fo),reverse=rev)[0:N]

reduced1=[]
reduced2=[]
reduced3=[]
reduced4=[]
reduced5=[]
j=0
count=dict()
for line in sys.stdin:
  j+=1
  line1 = line.strip()
  if line1.find( '<' + tag) != -1:
    parser = ET.XMLPullParser(['end'])
    parser.feed(line)
    for event, elem in parser.read_events():
      if elem.attrib=={}:
          break
      else:
        if elem.attrib['PostTypeId']=='1':
          if 'AcceptedAnswerId' in elem.attrib:
            mapped[elem.attrib['Id']]={'PostTypeId':elem.attrib['PostTypeId'],'ViewCount':elem.attrib['ViewCount']
            ,'AcceptedAnswerId':elem.attrib['AcceptedAnswerId']
            ,'CreationDate':elem.attrib['CreationDate'],
            'Score':elem.attrib['Score'],'Body':elem.attrib['Body']}
          else:
            mapped[elem.attrib['Id']]={'PostTypeId':elem.attrib['PostTypeId'],'ViewCount':elem.attrib['ViewCount']
            ,'AcceptedAnswerId':''
            ,'CreationDate':elem.attrib['CreationDate'],
            'Score':elem.attrib['Score'],'Body':elem.attrib['Body']}

        if elem.attrib['PostTypeId']=='2':
          mapped[elem.attrib['Id']]={'PostTypeId':elem.attrib['PostTypeId'],'ViewCount':elem.attrib['ViewCount']
          ,'AcceptedAnswerId':''
          ,'CreationDate':elem.attrib['CreationDate'],
          'Score':elem.attrib['Score'],'Body':elem.attrib['Body']}
      elem.clear()
    parser=None


for key, values in mapped.items():
  reduced1.append([key,values['ViewCount']])
 
  if values['PostTypeId']=='1':
    reduced2.append([values['Score'],values['Body'],key,values['PostTypeId']])
    if values['AcceptedAnswerId']!='':
      reduced4.append([values['Score'],key,values['PostTypeId'],values['CreationDate'],
        values['AcceptedAnswerId']])
  else:
    reduced3.append([values['Score'],values['Body'],key,values['PostTypeId']])
    reduced5.append([values['Score'],key,values['PostTypeId'],values['CreationDate'],
      values['AcceptedAnswerId']])

reduced_res1=dict()
reduced_res6=dict()
reduced_res7=dict()
reduced_res4=dict()
reduced_res5=dict()

if len(reduced1)>0:
  reduced_res1[1]=reduce_c_args(reducer_menores,reduced1 ,10,False,1)
  print(reduced_res1)
if len(reduced2)>0:
  reduced_res2=reduce_c_args(reducer_menores, reduced2,10,True,0)
  for i in range(10):
    reduced_res2[i][1]=re.sub(r'[^\w\s]','',re.sub('<.*?>','',reduced_res2[i][1]))
    reduced_res6[2]=[reduced_res2[i]]
    print(reduced_res6)
if len(reduced3)>0:
  reduced_res3=reduce_c_args(reducer_menores, reduced3,10,True,0)
  for i in range(10):
    reduced_res3[i][1]=re.sub(r'[^\w\s]','',re.sub('<.*?>','',reduced_res3[i][1]))
    reduced_res7[3]=[reduced_res3[i]]
    print(reduced_res7)
if len(reduced4)>0:
  reduced_res4[4]=reduce_c_args(reducer_menores, reduced4,400,True,0)
  print(reduced_res4)
if len(reduced5)>0:
  for i in range(len(reduced5)):
    reduced_res5[5]=[reduced5[i]]
    print(reduced_res5)
if count.get('j',0)==0:
  count['j']=j
else:
  count['j']=count['j'] + j
print(count)