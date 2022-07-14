import xml.etree.ElementTree as ET
import time
from datetime import datetime


start_time = time.time()

def text_cleaner(text):
    text = text.replace('-', ' ').replace('><', ' ').replace('<', '').replace('>', '')
    return text

def seconds_to_hours(seconds: int) -> str:
    """
    Convert seconds to hours, minutes, seconds
    :param: seconds to int
    :return: returns string with the seconds in hour, minutes, seconds format
    """
    hours = int(seconds / 60 / 60)
    seconds -= hours*60*60
    minutes = int(seconds/60)
    seconds -= minutes*60
    full_time = 'horas: ' + str(hours)+':'+str(minutes)+':'+str(seconds)
    
    return full_time

def map_xml():
    """
    Read an xml file and return a list of values.
    combination of values:
    [Tags(str), AnswerCount(int)]
    [Body(str), Score(int)]
    [Id(int), CreationDate(datetime)]
    [ParentId(int), CreationDate(datetime)]

    :return: List of valours
    
    """
    file = ET.parse("posts.xml")

    #File Body
    body = file.getroot()

    #File body headers
    headers = {}

    #List of results
    mapped = []

    #Comparison point for ID
    idi = 0
    idf = 0

    for header in body:
        headers = header.attrib
        
        #Load the values that have accepted comments and load their Tags and number of accepted comments
        if 'AcceptedAnswerId' in headers:
            mapped.append(['tags: ' + str(headers['Tags']), int(headers['AnswerCount'])])

        #Load the values that have a body and their score
        if 'Body' in headers:
            mapped.append(['body: ' + str(headers['Body']), int(headers['Score'])])

        #Load the values that have type 1 = post creation, I save the creation date and its ID
        if 'Id' in headers and str(headers['PostTypeId']) == '1':
            time_c = headers['CreationDate'].replace('T', ' ')
            time_c= time_c.split('.')[0]
            idi = int(headers['Id'])
        #Load the values that have type 2 = response to a post, I save the creation date and its ID
        if 'Id' in headers and str(headers['PostTypeId']) == '2':
            time_f = headers['CreationDate'].replace('T', ' ')
            time_f= time_f.split('.')[0]
            idf = int(headers['ParentId'])
        
        if idi == idf:
            mapped.append(['post: ' + time_c, 'answer: ' + time_f])
    return mapped

def reduce_top(mapped: list) -> list:
    """
    Returns a top 10 list of tags with the most accepted answers
    :param: list mapped
    :return: top 10 list
    """
    #top 10
    top_10= []

    #reference value
    comp = 0

    #we iterate through the list of values and look for the string with the word tags
    for list_values in mapped:
        if type(list_values[0]) == type(str()):
            if 'tags:' == list_values[0].split()[0]:
                if list_values[1] > comp:
                    #We make sure that there are only 10 values
                    if len(top_10) < 10:
                        top_10.append(['Respuestas aceptadas: ' + str(list_values[1]), text_cleaner (list_values[0])])
                        comp = list_values[1]
                    #Otherwise, we delete the smallest value from the list and add the new one.
                    else:
                        for value in top_10:
                            if min(top_10) < list_values[1] and not list_values[1] in top_10:
                                top_10.remove(value)
                                top_10.append(['Respuestas aceptadas: ' + str(list_values[1]), text_cleaner (list_values[0])])
                                comp= list_values[1]
    return top_10


def reduce_score(mapped: list) -> list:
    """
    Calculate the average number of words and score
    :param: mapped
    :return: average
    """
    score= 0
    letters= 0
    cont = 0
    averages = []
    for list_values in mapped:
        if type(list_values[0]) == type(str()):
            if 'body:' == list_values[0].split()[0]:
                if len(list_values[0].split()) > 0:
                    letters += len(list_values[0].split())
                    score += list_values[1]
                    cont += 1
    letter_average = round(letters/cont, 0)
    score_average = round(score/cont, 0)
    averages.append([letter_average, score_average])
    return averages

def reduce_answers(mapped: list) -> list:
    """
    Calculate the average response delay of a post
    :param: mapped
    :return: average
    """
    list_comp = []
    postdate = []
    list_diff = []
    
    for list_values in mapped:
        if type(list_values[0]) == type(str()):
            if 'post:' == list_values[0].split()[0]:
                list_comp.append([list_values[0], list_values[1]])

    for num in range(len(list_comp)):
        if list_comp[num][0] != list_comp[num-1][0]:
            post = list_comp[num][0].replace('post: ', '')
            post = datetime.strptime(post, '%Y-%m-%d %H:%M:%S')

            answer = list_comp[num][1].replace('answer: ','')
            answer = datetime.strptime(answer, '%Y-%m-%d %H:%M:%S')

            postdate.append([post, answer])

    for date in postdate:
        diff = date[1] - date[0]
        list_diff.append(diff)
    
    result_sum = min(list_diff)

    for result in list_diff:
        result_sum += result
    
    result_sum -= min(list_diff)
    result_sum = result_sum.total_seconds()

    div = len(list_diff)
    result = round(int(result_sum) / int(div))
    answer = seconds_to_hours(result)
    return answer

def print_top(top_10):
    print ("""
            Top 10 tags con mayores respuestas aceptadas
    """)
    for items in top_10:
        print (items[0],"---", items[1])
    print ("\n")

def print_average(averages):
    print ("""
            Relación entre cantidad de palabras en un post y su puntaje
    """)
    for average in averages:
        print(f"Se requieren {average[0]} palabras para obtener un score de {average[1]}")
    print ("\n")

def print_answers(answers):
    print ("""
            Demora de respuesta promedio en posts
    """)
    print (answers)
    print ("\n")

def main ():
    mapped= map_xml()
    top_10 = reduce_top(mapped)
    score = reduce_score(mapped)
    answers = reduce_answers(mapped)
    print_top(top_10)
    print_average (score)
    print_answers(answers)

    stop_time= (time.time() - start_time)
    print("Tiempo de ejecucion:", stop_time)

if __name__ == '__main__':
    main()