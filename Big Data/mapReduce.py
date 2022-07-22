import xml.etree.ElementTree as ET    
from collections import Counter
import re
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import wait
import time
import operator
import math

# se busca los caracteres que son innecesarios para el tag
tagRegex = re.compile("<([^>]+)>")
# ubicacion del archivo xml
pathXml ='/home/sabrina/Big Data/input/posts.xml'

#Para tomar el tiempo de ejecucion
start_time = time.time()

# Obtengo los tags que tuvieron respuestas aceptadas
def tags_accepted_answers(data):
   
        postsType = data.attrib.get('PostTypeId')
      
        # Si el tipo del post no es pregunta, lo ignoro
        if postsType != "1":
            return
        else:
            try:
                acceptedId = data.attrib.get('AcceptedAnswerId')
                tags = data.attrib.get('Tags')
                    
            except Exception as e:
                    print(f"Error al obtener datos: {e}")
            else:
                # Acumular solo si la respuesta es aceptada
                tagList = tagRegex.findall(tags)
                if acceptedId:
                    return tagList 
          
  
# Aplica a cada conjunto de datos la función de extracción
def tag_mapper(data):
    
        tagAcept = list(map(tags_accepted_answers,data))
        tagAcept = list(filter(None,tagAcept))
        #reduzco la lista
        data_process = [item for sublist in tagAcept for item in sublist]
        return dict(Counter(data_process))    


# Por cada tag, la función suma la cantidad de respuestas aceptadas
def add_reducer(data):
    dictAux={}
    current_elem = None
    current_count = 0      
    for key, value in data.items():
        if current_elem == None: # primera aparicion de la clave
            current_elem = key
            current_count = value
            data.update({current_elem:current_count})
                # Si se tienen claves iguales, actualiza el valor de la clave repetida y las suma
        if current_elem == key :
            current_elem = key
            current_count += value
            data.update({current_elem:current_count})
        if current_elem != key: 
            current_elem = None   
    
        if not isinstance(data[key], int) or not isinstance(value, int):
                print(
                    f'Los valores en los diccionarios deben ser del tipo "int" {TypeError}'
                )     
    return data

# De acuerdo a los datos resultantes, calcula el top 10
def calculate_top_10(data1):
    
        return dict(Counter(data1).most_common(10))


      

# aplico a cada conjunto de datos la función de extracción
def user_mapped(pathXMl):
    
    list_aux = []
    for event,item in ET.iterparse(pathXml):
       
        # Si el tipo del postes pregunta, lo proceso
        try:
            post_type = item.attrib.get("PostTypeId")
            if post_type == "1":
                owner_id = item.attrib.get("OwnerUserId")
                fav_counts = item.attrib.get("FavoriteCount")
                
        except Exception as e:
                print(f"Error al obtener datos de usuario: {e}")
        else: 
            if owner_id and fav_counts:
                list_aux.append({owner_id:(int(fav_counts))})
    return list_aux           
               
# Se procesa el mapped oteniendo como resultado un diccionario con clave valor
# si la claves son iguales las cantidad de respuestas favoritas por cada usuario
def user_reduce(data):
    dictAux={}
    current_elem = None
    current_count = 0
    for elem in data:
        for key,value in elem.items():
            if current_elem == None: # primera aparicion de la clave
                current_elem = key
                current_count = value
                dictAux.update({current_elem:current_count})
            if current_elem == key :# se repiten la claves
                    current_elem = key
                    current_count += value
                    dictAux.update({current_elem:current_count})
            if current_elem != key: 
                    current_elem = None   

            if not isinstance(elem[key],int) or not isinstance(value,int):
                    print(
                                f'Los valores en los diccionarios deben ser del tipo "int" {TypeError}'
                    )
    return(dictAux)                        

# De acuerdo a los datos resultantes, calcula el top 10
def calcule_top_10_users(data):

    users_sort = sorted(data.items(), key=operator.itemgetter(1), reverse=True)
    return(users_sort)

# Obtengo la relación entre la cantidad de palabra de cada post y la cantidad de respuestas que obtuvo
def relation_mapped(pathXml):
    list_aux = []
    for event,item in ET.iterparse(pathXml):
    
        try:
            post_type = item.attrib.get("PostTypeId")
            if post_type == "1":
                answer_count = item.attrib.get("AnswerCount")
                body = item.attrib.get("Body")  
        except Exception as e:
            print(f"Error al obtener datos: {e}")
        else:
             words_count = len(body)
             if answer_count and body:
                answer_count = int(answer_count)
                # Si tuvo respuestas
                if answer_count != 0:
                # Devuelvo la cantidad de palabras y la cantidad de respuestas
                    list_aux.append({words_count: answer_count})
    return list_aux              

#  Promedio la cantidad de respuestas por la cantidad de palabras del posts
def relation_reducer(data):
    dictAux={}
    current_elem = None
    current_count = 0
    for elem in data:
        for key,value in elem.items():
            if current_elem == None: # primera aparicion de la clave
                current_elem = key
                current_count = value
                dictAux.update({current_elem:current_count})
            if current_elem == key :# se repiten la claves
                current_elem = key
                current_count = elem[key]+ value
                # ceil: Redondea un número hacia arriba a su entero más cercano
                dictAux.update({current_elem:int(math.ceil((current_elem)/current_count))})
            if current_elem != key: 
                current_elem = None      
            if not isinstance(elem[key], int) or not isinstance(value, int):
                print(
                        f'Los valores en los diccionarios deben ser del tipo "int" {TypeError}'
                )              
    return dictAux

def calcule_top_10_users(data):

    users_sort = sorted(data.items(), key=operator.itemgetter(1), reverse=True)
    return(users_sort)

def task(task): 
    try:
        if task == 0:
            tree = ET.parse(pathXml)
            root  = tree.getroot()
            mappedPosts = tag_mapper(root)
            reducePosts= add_reducer(mappedPosts)
            top10 = calculate_top_10(reducePosts)
            df = pd.DataFrame(top10.items(), columns=['Tag', 'Cantidad respuestas aceptadas'])
            print('------- Top 10 tipo post con mayor respuesta aceptadas  -------')
            print(df)
        if task == 1:
            mappedFav = user_mapped(pathXml)
            reduceFav = user_reduce(mappedFav)
            user_fav= calcule_top_10_users(reduceFav)
            dfUsers = pd.DataFrame(user_fav,columns=['idUsers', 'Cantitidad de respuestas favoritas']).head(10)
            print('------- Top 10 con mayor porcentaje de respuestas favoritas  -------')
            print(dfUsers)
        if task == 2:    
            mappedRelation = relation_mapped(pathXml)
            reduceRelation = relation_reducer(mappedRelation)
            Rel= calcule_top_10_users(reduceRelation)
            dfRel = pd.DataFrame(Rel,columns=['Cantidad de palabras' , 'Relacion cantidad coon la respuestas'])
            print('------- Relacion entre cantidad de palabras en un post y su cantidad de respuestas  -------')
            print(dfRel)
    except Exception as e:
        print(e)

def main():
     # Iniciar el proceso de pool
    with ProcessPoolExecutor() as executor:
        # pasar tareas a traves de la funcion task y esperar los resultados
        futures = [executor.submit(task, i) for i in range(3)]
        # Esperar que se reciban todos los resultados
        wait(futures)      
    #Para ver el tiempo que tardo la ejecucion
    print("Tiempo de ejecucion--- %s seconds ---" % (time.time() - start_time))
 
if __name__ == '__main__':
    main()    
