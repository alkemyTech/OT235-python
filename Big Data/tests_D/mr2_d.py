# -*- coding: utf-8 -*-
import xml.etree.ElementTree as ET
from operator import itemgetter
import sys
import pandas as pd
import ast
import re
from collections import Counter
#from nltk.corpus import stopwords
from datetime import datetime as dt
import time

#Para tomar el tiempo de ejecucion
start_time = time.time()

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


for event, elem in ET.iterparse("/home/sabrina/Big Data/input/posts.xml"):
  if elem.attrib=={}:
      break
  else:

    if elem.attrib['PostTypeId']=='1':

      if 'AcceptedAnswerId' not in elem.attrib and 'AnswerCount' not in elem.attrib:
        mapped[elem.attrib['Id']]={'PostTypeId':elem.attrib['PostTypeId'],'AnswerCount':''
        ,'AcceptedAnswerId':''
        ,'CreationDate':elem.attrib['CreationDate'],
        'Score':elem.attrib['Score']}
      elif 'AcceptedAnswerId' not in elem.attrib and 'AnswerCount' in elem.attrib:
        mapped[elem.attrib['Id']]={'PostTypeId':elem.attrib['PostTypeId'],'AnswerCount':elem.attrib['AnswerCount']
        ,'AcceptedAnswerId':''
        ,'CreationDate':elem.attrib['CreationDate'],
        'Score':elem.attrib['Score']}
      elif 'AcceptedAnswerId' in elem.attrib and 'AnswerCount' in elem.attrib:
        mapped[elem.attrib['Id']]={'PostTypeId':elem.attrib['PostTypeId'],'AnswerCount':elem.attrib['AnswerCount']
        ,'AcceptedAnswerId':elem.attrib['AcceptedAnswerId']
        ,'CreationDate':elem.attrib['CreationDate'],
        'Score':elem.attrib['Score']}
      elif 'AcceptedAnswerId' in elem.attrib and 'AnswerCount' not in elem.attrib:
          mapped[elem.attrib['Id']]={'PostTypeId':elem.attrib['PostTypeId'],'AnswerCount':''
        ,'AcceptedAnswerId':elem.attrib['AcceptedAnswerId']
        ,'CreationDate':elem.attrib['CreationDate'],
        'Score':elem.attrib['Score']}
    if elem.attrib['PostTypeId']=='2':
      mapped[elem.attrib['Id']]={'PostTypeId':elem.attrib['PostTypeId'],'AnswerCount':''
      ,'AcceptedAnswerId':''
      ,'CreationDate':elem.attrib['CreationDate'],
      'Score':elem.attrib['Score']}

  elem.clear()


for key, values in mapped.items():

  if values['PostTypeId']=='1':
    #Para relacion entre respuestas y score
    if values['AnswerCount']!='' and values['AnswerCount']!='0':
        reduced1.append([round(int(values['Score']) / int(values['AnswerCount']),4),key])
    #Para top 10 sin respuestas aceptadas
    if values['AcceptedAnswerId']=='' or values['AcceptedAnswerId']==' ':
      reduced2.append([values['Score'],key,values['PostTypeId']])
    #Para top 10 con mayor tiempo de respuesta con respuesta aceptada
    if values['AcceptedAnswerId']!='':
      reduced4.append([values['Score'],key,values['PostTypeId'],values['CreationDate'],
      values['AcceptedAnswerId']])
  else:
    reduced5.append([values['Score'],key,values['PostTypeId'],values['CreationDate'],
    values['AcceptedAnswerId']])
 
  

reduced_res1=dict()
reduced_res2=dict()
reduced_res4=dict()
reduced_res5=dict()
prom=0
counts=0

if len(reduced1)>0:
  for i in range(len(reduced1)):
    prom+=reduced1[i][0]
    counts+=1
  reduced_res1=[prom,counts]
if len(reduced2)>0:
  reduced_res2=reduce_c_args(reducer_menores, reduced2,10,True,0)
if len(reduced4)>0:
  reduced_res4=reduced4
if len(reduced5)>0:
  for i in range(len(reduced5)):
    reduced_res5[reduced5[i][1]]=reduced5[i]



    
resultado=dict()

i1=0

i2=0

i3=0

i4=0

i5=0
i6=0
i7=0

#reduced400 = reduce_c_args(reducer_menores, reduced_res4,400,True,0)
#reduced4 = reduce_c_args(reducer_menores, reduced400,200,False,0)
#df_reduced3=pd.DataFrame(reduced4, columns=['Score','Id','PostTypeId','CreationDate','AcceptedAnswerId'])
#df_reduced3['CreationDate']=df_reduced3['CreationDate'].apply(lambda y :dt.fromisoformat(y))

for x in range(len(reduced_res4)):
    if i7==0:
        df_reduced3=pd.DataFrame([reduced_res4[x]], columns=['Score','Id','PostTypeId','CreationDate','AcceptedAnswerId'])
        i7+=1
    else:
        try:
            df_reduced3.loc[len(df_reduced3)]=reduced_res4[x]
        except:
            continue
        i7+=1

reduced_res4=0

for x in df_reduced3['AcceptedAnswerId']:
    if i5==0:
        df_reduced4=pd.DataFrame([reduced_res5[str(x)]], columns=['Scorer','Idr','PostTypeIdr','CreationDater','AcceptedAnswerIdr'])
        i5+=1
    else:
        try:
            df_reduced4.loc[len(df_reduced4)]=reduced_res5[str(x)]
        except:
            continue
        i5+=1

reduced_res5=0

prom=0
counts=0
if isinstance(reduced_res1[1],int):
    prom+=reduced_res1[0]
    counts+=reduced_res1[1]
else:
    for x in range(len(reduced_res1)):
        prom+=reduced_res1[x][0]
        counts+=reduced_res1[x][1]

rel=round(prom/counts,0)
#reduced1 = reduce_c_args(reducer_menores, reduced_res1,10,False,1)
print(f'Relacion entre score y respuestas: en Promedio por cada respuesta corresponden {rel} puntos')
reduced2 = reduce_c_args(reducer_menores, reduced_res2,10,True,0)
r_df=pd.DataFrame(reduced2, columns=['Score','Id','PostTypeId'])
print('Top 10 mas score sin respuestas aceptadas', r_df[['Id','Score']])


#reduced5 = reduce_c_args(reducer_menores, reduced_res5,len(reduced_res5),False,0)
df_reduced3['CreationDate']=pd.to_datetime(df_reduced3['CreationDate'].apply(lambda y :dt.fromisoformat(y)))
df_reduced4['CreationDater']=pd.to_datetime(df_reduced4['CreationDater'].apply(lambda y :dt.fromisoformat(y)))
df_dif=pd.merge(df_reduced3,df_reduced4,how='left',left_on=['AcceptedAnswerId'],right_on=['Idr'])
df_reduced3=0
df_reduced4=0
df_dif['dif']=df_dif['CreationDater']-df_dif['CreationDate']
df_dif_final=df_dif.sort_values(by='dif',ascending=False).head(10)
print('Top 10 post con mayor actividad hasta respuesta correcta',df_dif_final[['Id','dif']])
#Para ver el tiempo que tardo la ejecucion
print("Tiempo de ejecucion--- %s seconds ---" % (time.time() - start_time))
