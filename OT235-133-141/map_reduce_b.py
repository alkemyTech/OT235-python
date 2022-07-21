import xml.etree.ElementTree as ET
import time
from datetime import datetime
from collections import defaultdict


start_time = time.time()


def map_xml() -> dict:
    """
    It returns preselected data in a one-to-one relationship for the resolution of the following problems:
        1) Top 10 tags with no accepted answers
        2) Relationship between the number of words in a post and its number of visits
        3) Average score of the answers with the most favorites


    :return: dictionary with dictionaries:
                                            tags_no_replies
                                            quantity_and_visits
                                            average_responses_favorites
    """
    file = ET.parse("posts.xml")

    #File Body
    body = file.getroot()

    #File body headers
    header = {}

    #Dictionary with selected data results
    tags_no_replies = {}
    quantity_and_visits = {}
    average_responses_favorites = {}

    #Dictionary with all dictionaries and their results
    mapped = {}

    idi = 0
    idf = 0

    for headers in body:
        header = headers.attrib

        if int (header['PostTypeId']) == 1 and 'Tags' in header:
            if 'AcceptedAnswerId' in header:
                tags_no_replies['tags_no_replies ' + str(header['Id'])] = [str(header['Tags']), 1]
            else:
                tags_no_replies['tags_no_replies ' + str(header['Id'])] = [str(header['Tags']), 0]
            
        #We select only the values with PostTypeId=1
        if int (header['PostTypeId']) == 1:
            #We load your Body and ViewCount
            quantity_and_visits['quantity_and_visits ' + str(header['Id'])] = [str(header['Body']), int(header['ViewCount'])]
        
        if int (header['PostTypeId']) == 1 and 'FavoriteCount' in header:
            average_responses_favorites['average_responses_favorites_p '+ str(header['Id'])] = [int(header['Id']), int(header['FavoriteCount'])]
        
        if int (header['PostTypeId']) == 2 and 'Score' in header:
            average_responses_favorites['average_responses_favorites_r ' + str(header['Id'])] = [int(header['ParentId']), int(header['Score'])]
    
    #Assignment of dictionaries and their values
    mapped['tags_no_replies'] = tags_no_replies
    mapped['quantity_and_visits'] = quantity_and_visits
    mapped['average_responses_favorites'] = average_responses_favorites

    return mapped

def reduce_top(mapped: dict) -> list:
    """
    Calculate the tops of tags without accepted answers
    :param: mapped
    :return: top_10
    """
    dic_tag = {}
    top_10 =[]
    comp = 1000

    """ 
    Look in the dictionary for the keyword 'tags_no_replies' and make a dictionary 
    of these in tag-value relationship according to the number of times it is repeated
    """
    for keys in mapped:
        #selection
        if keys == 'tags_no_replies':
            for keya, item in mapped[keys].items():
                #0 = post with no accepted replies
                if item[1] == 0:
                    #stock loading
                    if item[0] not in dic_tag.keys():
                        dic_tag[item[0]] = 1
                    else:
                        dic_tag[item[0]] += 1
    for val in dic_tag:
        if dic_tag[val] <= comp:
            if len(top_10) < 10:
                top_10.append([val, dic_tag[val]])
                comp = dic_tag[val]
            else:
                for n in range(len(top_10)):
                    if top_10[n][1] > dic_tag[val]:
                        top_10.remove(top_10[n])
                        top_10.append([val, dic_tag[val]])
                        comp = dic_tag[val]
    return top_10 

def reduce_words(mapped: dict) -> list:
    """
    Calculates the average/relationship between the number of words in a post 
    and its number of views

    :param: mapped
    :return: number_words_score[letter, view]
    """
    number_words_score = []
    letters = 0
    view = 0
    div = 0

    for keys in mapped:
        #selection
        if keys == 'quantity_and_visits':
            #we count the number of letters, number of views and add our divisor according to the number of posts
            for keya, item in mapped[keys].items():
                letters += (len(item[0].split()))
                view += item[1]
                div += 1

    letter_average = round(letters/div, 0)
    view_average = round(view/div, 0)
    number_words_score.append([letter_average, view_average])

    return number_words_score


def reduce_favorite(mapped: dict ) -> list:
    """
    Perform the search, sum and division of all the posts with 
    the most favorites greater than 50

    :param: mapped
    :return: average_list['number_of_responses', Score]
    """
    #lists with selected values
    list_id_values = []
    list_id_favorite = []

    #dictionaries resulting from lists
    dict_value_div = {}
    dict_value = {}

    #resulting list with average
    list_result = []

    #resulting list with the average and amount of score
    average_list = []

    #create list of values and favorites
    for keys in mapped:
        #selection average_responses_favorites
        if keys == 'average_responses_favorites':
            for keya, item in mapped[keys].items():
                #selection average_responses_favorites_p = favorite answers post
                if 'average_responses_favorites_p' in keya:
                    #we will take the posts with more than 50 favorites
                    if item[1] > 50:
                        list_id_values.append(item[0])
                        list_id_favorite.append([item[0], item[1]])


    #creating dictionaries with values and divisors
    for keys2 in mapped:
        #selection average_responses_favorites
        if keys2 == 'average_responses_favorites':
            for keya2, item2 in mapped[keys2].items():
                #selection average_responses_favorites_r = favorite answers
                if 'average_responses_favorites_r' in keya2:
                    for value_id in list_id_values:
                        if value_id == item2[0]:
                            if item2[0] not in dict_value:
                                dict_value[item2[0]] = item2[1]
                                dict_value_div[item2[0]] = 1
                            else:
                                dict_value[item2[0]] += item2[1]
                                dict_value_div[item2[0]] += 1
    
    #result of the union of the two dictionaries with the same keys
    result_dic = defaultdict(list)

    #union of dictionaries
    dictionaries = [dict_value, dict_value_div]
    for dic in dictionaries:
        for k, v in dic.items():
            result_dic[k].append(v)
    
    #division for average score
    for ke in result_dic:
        list_result.append([ke, round(result_dic[ke][0]/result_dic[ke][1])])
    
    #number of favourites-score ratio result
    for key3 in list_result:
        for key4 in list_id_favorite:
            if key3[0] == key4[0]:
                average_list.append([key4[1], key3[1]])

    return average_list

def print_top_10(top_10):
    print("""
        ***************************************
        ** Top tags sin respuestas aceptadas **
        *************************************** """)
    for item in top_10:
        print(f"""tags: {item[0]} <----> respuesta aceptadas: {item[1]}
        """)

def print_words_view(words_view):
    print("""
        *******************************************************
        ** Relacion entre cantidad de palabras y sus visitas **
        ******************************************************* """)
    for item in words_view:
        print(f"""Los post con un promedio de palabras de {int(item[0])} obtienen un promedio de visitas de {int(item[1])}
        """)

def print_average_response_favorite(average_response_favorite):
    print("""
        *********************************************************
        ** Puntaje promedio de las respuestas con mas favoritos**
        ********************************************************* """)
    for item in average_response_favorite:
        print(f""" favoritos {item[0]} <----> promedio {item[1]}
        """)

def main ():
    mapped = map_xml()
    top_10 = reduce_top(mapped)
    words_view = reduce_words(mapped)
    average_response_favorite = reduce_favorite(mapped)
    print_top_10(top_10)
    print_words_view(words_view)
    print_average_response_favorite(average_response_favorite)
    stop_time= (time.time() - start_time)
    print("Tiempo de ejecucion:", stop_time)

if __name__ == '__main__':
    main()