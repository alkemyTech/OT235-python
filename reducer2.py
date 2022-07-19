# -*- coding: utf-8 -*-

import sys
import pandas as pd
import ast
import re
from collections import Counter
from nltk.corpus import stopwords
from datetime import datetime as dt
import time
from operator import itemgetter

#Para tomar el tiempo de ejecucion
start_time = time.time()


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

#funcion para quitar caracteres que no sean claros
def limpiar_palabra(palabra):
    return re.sub(r'[^\w\s]','',palabra).lower()

#Funcion para eliminar stopwords y las que no son alfabeticas
def palabra_no_stop(palabra):
    return palabra not in stopwords.words() and palabra.isalpha()

#mapear y filtro de palabras
def palabras_top(data):
    cnt = Counter()
    for text in data:
        tokens_in_text = text.split()
        tokens_in_text = map(limpiar_palabra, tokens_in_text)
        tokens_in_text = filter(palabra_no_stop, tokens_in_text)
        cnt.update(tokens_in_text)
    return cnt.most_common(10)
    
resultado=dict()
reduced_res1=[]
i1=0
reduced_res2=[]
i2=0
reduced_res3=[]
i3=0
reduced_res4=[]
i4=0
reduced_res5=dict()
i5=0
i6=0
# input comes from STDIN
for line in sys.stdin:
    line=ast.literal_eval(line)
    # remove leading and trailing whitespace
    if len(line.get(1, ''))>0:
        if i1==0:
            reduced_res1=line[1]
            i1+=1
        else:
            reduced_res1=reduced_res1 + line[1]
            i1+=1
    if len(line.get(2, ''))>0:
        if i2==0:
            reduced_res2=line[2]
            i2+=1
        else:
            reduced_res2=reduced_res2 + line[2]
            i2+=1
    if len(line.get(3, ''))>0:
        if i3==0:
            reduced_res3=line[3]
            i3+=1
        else:
            reduced_res3=reduced_res3 + line[3]
            i3+=11
    if len(line.get(4, ''))>0:
        if i1==0:
            reduced_res4=line[4]
            i4+=1
        else:
            reduced_res4=reduced_res4 + line[4]
            i4+=1
    if len(line.get(5, ''))>0:
        reduced_res5[line[5][0][1]]=line[5]

    if line.get('j', 0)>0:
        if i6==0:
            count=line['j']
            i6+=1
        else:
            count=count + line['j']
            i6+=1


reduced400 = reduce_c_args(reducer_menores, reduced_res4,400,True,0)
reduced4 = reduce_c_args(reducer_menores, reduced400,200,False,0)
df_reduced3=pd.DataFrame(reduced4, columns=['Score','Id','PostTypeId','CreationDate','AcceptedAnswerId'])
df_reduced3['CreationDate']=df_reduced3['CreationDate'].apply(lambda y :dt.fromisoformat(y))

for x in df_reduced3['AcceptedAnswerId']:
    if i5==0:
        df_reduced4=pd.DataFrame([reduced_res5[x][0]], columns=['Scorer','Idr','PostTypeId','CreationDater','AcceptedAnswerIdr'])
        i5+=1
    else:
        try:
            df_reduced4.loc[len(df_reduced4)]=reduced_res5[x][0]
        except:
            continue
        i5+=1

reduced1 = reduce_c_args(reducer_menores, reduced_res1,10,False,1)
resultado['Top 10 menos Vistos']=[pd.DataFrame(reduced1, columns=['Id','Vistas'])]
reduced2 = reduce_c_args(reducer_menores, reduced_res2,10,True,0)
#convierto en dataframe para trabajar el body de los posts
df_reduced=pd.DataFrame(reduced2, columns=['Score','Body','Id','PostTypeId'])
#Obtengo las 10 palabras top por tipo de post
resultado['Top 10 palabras en 10 post mas score Tipo Post=1']=palabras_top(df_reduced['Body'].astype(str).apply(lambda y :re.sub('<.*?>','',y)))
reduced3 = reduce_c_args(reducer_menores, reduced_res3,10,True,0)
df_reduced2=pd.DataFrame(reduced3, columns=['Score','Body','Id','PostTypeId'])
#Obtengo las 10 palabras top por tipo de post
resultado['Top 10 palabras en 10 post mas score Tipo Post=2']=palabras_top(df_reduced2['Body'].astype(str).apply(lambda y :re.sub('<.*?>','',y)))


#reduced5 = reduce_c_args(reducer_menores, reduced_res5,len(reduced_res5),False,0)
df_reduced4['CreationDater']=df_reduced4['CreationDater'].apply(lambda y :dt.fromisoformat(y))
df_dif=pd.merge(df_reduced3,df_reduced4,how='left',left_on=['AcceptedAnswerId'],right_on=['Idr'])
df_dif=df_dif.sort_values(by='Score').head(100)
res_final=int(round(((df_dif['CreationDater']-df_dif['CreationDate']).mean()).total_seconds() / 60 , 0))
resultado['Minutos promedio de respuesta para los Top 300-400 post por score']=[res_final]

for x in resultado:
    print(x)
    for y in resultado[x]:
        print(y)
#Para ver el tiempo que tardo la ejecucion
print("Tiempo de ejecucion--- %s seconds ---" % (time.time() - start_time))
print("Registros leidos:  %s " % count)
#Convierto en dataframe para trabajar el creation date y el 'AcceptedAnswerId' de la respuesta
