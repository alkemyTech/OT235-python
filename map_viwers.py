import xml.etree.ElementTree as ET
import logging


#Importamos el archivo xml y lo decidificamos
xml_file = "/mnt/a/files_acceleracion/metastackoverflow/posts.xml"
tree = ET.parse(xml_file)
root = tree.getroot()

""" Funcion que recorre los atributos de cada row en el xml y extrae los tag id y viewers
    para llevarlos a una lista
"""
def id_list_dic():
    try:
        list_viewers = []
        for event,item in ET.iterparse(xml_file):
            Id = item.attrib.get("Id")
            ViewCount = item.attrib.get("ViewCount")
            if Id != None and ViewCount != None:
                list_viewers.append({'id':Id,'viewers':ViewCount})
        return list_viewers
    
    except:
        logging.error('No se pudiero extraer los atributos del XML')


print(id_list_dic())