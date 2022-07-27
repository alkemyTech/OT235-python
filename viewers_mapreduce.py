import xml.etree.ElementTree as ET
import logging
import operator
import time


start_time = time.time()
xml_file = "/mnt/a/files_acceleracion/metastackoverflow/posts.xml"
tree = ET.parse(xml_file)
root = tree.getroot()


'''Funcion que extrae los datos a utilizar y devuelve un diccionario id : viewers
    input: xml_file
    output: data
'''
def viewers_data():
    try:
        data = {}
        for event,item in ET.iterparse(xml_file):
            Id = item.attrib.get("Id")
            ViewCount = item.attrib.get("ViewCount")
            if Id != None and ViewCount != None:
                data.update({int(Id):int(ViewCount)})
        return data
    except:
        logging.error('No se pudiero extraer los atributos del XML')


''' Funcion que ordena y retorna los 10 valores mas altos del diccionario
    input: dict 
    output: top_10
'''
def top_10_data(n: dict):
    top_10 = dict(sorted(n.items(), key=operator.itemgetter(1), reverse=True)[:10])
    return top_10


top_10 = top_10_data(viewers_data())
print(f'Top 10 Posts mas vistos:\nid:viewers {top_10}')
print("Tiempo de ejecucion:  %s segundos: " % (time.time() - start_time))