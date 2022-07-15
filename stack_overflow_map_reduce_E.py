import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from collections import Counter
from functools import reduce
from functools import partial
from multiprocessing import Pool
import logging
from logging.config import fileConfig

# log implementation
CONFIG_FILE = 'log_Big_Data.cfg'
fileConfig(CONFIG_FILE)
logger = logging.getLogger()

# Constant definitions
DIR = Path(__file__).parent
FOLDER = '/112010 Meta Stack Overflow/'
FILE_DIC = {'badges': 'badges.xml',
            'comments': 'comments.xml',
            'posthistory': 'posthistory.xml',
            'posts': 'posts.xml',
            'users': 'users.xml',
            'votes': 'votes.xml'
            }
DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
TOP_N = 10
TYPE_QUESTION = 1
TYPE_ANSWER = 2
SCORE_MIN = 0
SCORE_MAX = 100


def get_data(file_name):
    ''' Get post data from file_name file, returning 
        all rows.
    '''
    try:
        FILE_PATH = str(DIR) + FOLDER + FILE_DIC[file_name]

        tree = ET.parse(FILE_PATH)
        root = tree.getroot()
        data = root.findall('row')
        return data
    except (FileNotFoundError, IOError):
        logger.debug('Wrong file name or file path')


def divide_chunks(li, n):
    ''' Generator for the division of list 'li' into
        sublists of n elements
    '''
    for i in range(0, len(li), n):
        yield li[i:i + n]


def get_creation_date(data):
    '''Creation of list with creation dates of post elements
        in data list.
    '''
    try:
        creation_date = [datetime.strptime(
            post.attrib['CreationDate'], DATE_FORMAT).date() for post in data]
        return creation_date
    except ValueError as ve:
        logger.debug(f'ValueError Raised: {ve}')


def count_creation_dates(chunk):
    '''Count the repetitions of post creation dates
    '''
    creation_date = get_creation_date(chunk)
    return Counter(creation_date)


def reducer_dates(count_1, count_2):
    '''Update the count of dates stored in count_1
       with dates counted in count_2
    '''
    count_1.update(count_2)
    return count_1


def top_dates_created_posts(file_type):
    '''Main function to calculate the top 10 dates
       for the creation of posts.
    Parameters:
        file_type: key from FILE_DIC
    '''
    data = get_data(file_type)
    if len(data) < TOP_N:
        logger.debug(f'Not enough posts for a top {TOP_N}')
        print(f'Not enough posts for a top {TOP_N}')
        return
    pool = Pool(4)
    # n is the number of elements in each chunk
    n = 100
    # generation of chuncks
    data_chunks = list(divide_chunks(data, n))
    # Map-reduce process
    mapped = pool.map(count_creation_dates, data_chunks)
    mapped = list(mapped)
    reduced = reduce(reducer_dates, mapped)
    # Selection of top 10 most repeated dates
    top = reduced.most_common(TOP_N)

    print(f'Top 10 dates   Created posts')
    for n in top:
        print(f'{n[0]}:     {n[1]}')


def get_view_counts(data):
    '''Gets the Post Type and the view count of every
       post in data and cast them.
       Returns a list of tuples.
    '''
    type_view = []
    for post in data:
        try:
            post_type = int(post.attrib['PostTypeId'])
            view_count = int(post.attrib['ViewCount'])
            type_view.append((post_type, view_count))
        except TypeError as err:
            logger.debug(err)
    return type_view


def reduce_count_view(mapped_type_view):
    '''By checking the PostTypeId (type_view[0]), it 
       counts the number of answers and the number of views
       of question posts.
       Returns a sigle tuple with the total number of answers 
       and the total views of all question posts.
    '''
    count_views = []
    count_answers = 0
    for type_view in mapped_type_view:
        if type_view[0] == TYPE_QUESTION:
            count_views.append(type_view[1])
        elif type_view[0] == TYPE_ANSWER:
            count_answers += 1
        else:
            pass
    return (count_answers, sum(count_views))


def count_mapper(data_chunks):
    '''For a sigle list of post in data_chunks, it
       maps the posts and reduces to a sigle tuple
       with the total number of answers
       and the total views of all question posts.
    '''
    mapped_type_view = get_view_counts(data_chunks)
    count = reduce_count_view(mapped_type_view)
    return count


def reducer(answ_views_1, answ_views_2):
    '''Gets two tuples, answ_views_1 and answ_views_2
       and returns the sum of each of their elements.
    '''
    sum_answ = answ_views_1[0] + answ_views_2[0]
    sum_views = answ_views_1[1] + answ_views_2[1]
    return (sum_answ, sum_views)


def get_answers_views_ratio(file_type):
    '''Main function to calculate the view-to-answer
       ratio from StackOverflow posts. 
    '''
    data = get_data(file_type)
    pool = Pool(4)
    # n is the number of elements in each chunk
    n = 1000
    data_chunks = list(divide_chunks(data, n))
    mapped = pool.map(count_mapper, data_chunks)
    mapped = list(mapped)
    reduced = reduce(reducer, mapped)
    try:
        # computation of the ratio
        total_answer_count = reduced[0]
        total_view_count = reduced[1]
        ratio = total_view_count/total_answer_count
        print(f'Views-to-answers ratio: {ratio:.2f}')
    except ZeroDivisionError:
        logger.debug('Cannot calculate ratio due to zero answers')


def get_question_posts(data):
    '''Filter posts in data by question type posts and
       by question posts with accepted answer.
       Returns a list of tuples with the Accepted Answer Id 
       and the Creation Date of the corresponding question post
       when the question score is between SCORE_MIN and SCORE_MAX
    '''
    question_posts = []
    for post in data:
        try:
            if int(post.attrib['PostTypeId']) == TYPE_QUESTION and 'AcceptedAnswerId' in post.attrib and 'Score' in post.attrib:
                if int(post.attrib['Score']) >= SCORE_MIN and int(post.attrib['Score']) <= SCORE_MAX:
                    question_posts.append(
                        (post.attrib['AcceptedAnswerId'], post.attrib['CreationDate']))
        except TypeError as err:
            logger.debug(err)
    return question_posts


def get_answer_response_time(data_chunk, questions):
    '''Filter the answer posts by the accepted answer posts and link
       them to the corresponding question post to calculate the difference 
       in time between the question created date and the accepted answer post
       date.
       Returns a list of response time for every question post with accepted
       anwser.
    '''
    accepted_answer_id = [tuple[0] for tuple in questions]
    response_times = []
    for post in data_chunk:
        try:
            if int(post.attrib['PostTypeId']) == TYPE_ANSWER and post.attrib['Id'] in accepted_answer_id:
                question_index = accepted_answer_id.index(post.attrib['Id'])
                question_creation_date = questions[question_index][1]
                question_creation_date = datetime.strptime(
                    question_creation_date, DATE_FORMAT)
                answer_creation_date = post.attrib['CreationDate']
                answer_creation_date = datetime.strptime(
                    answer_creation_date, DATE_FORMAT)
                res_time = (answer_creation_date -
                            question_creation_date).total_seconds()
                if res_time > 0:
                    response_times.append(res_time)
                else:
                    response_times.append(0)
        except TypeError as err:
            logger.debug(err)
    return response_times


def reducer_mean_time(times_list):
    '''Calculate the mean time for a list
        of lists with post response times.
    '''
    all_times = [
        time for list_time in times_list for time in list_time if time > 0]
    len_times = len(all_times)
    try:
        return sum(all_times)/len_times
    except ZeroDivisionError as err:
        logger.debug(err)


def get_mean_time_response(file_type):
    '''Mean function to calculate the mean response time in
       question posts with scores bewteen SCORE_MIN and SCORE_MAX
       using only accepted answers.
    '''
    data = get_data(file_type)
    question_posts = get_question_posts(data)
    pool = Pool(4)
    # n is the number of elements in each chunk
    n = 1000
    data_chunks = list(divide_chunks(data, n))
    # map of data_chunks passing the question posts list as fixed arg.
    mapped = pool.map(partial(get_answer_response_time,
                      questions=question_posts), data_chunks)
    mapped = list(mapped)
    mean_time = reducer_mean_time(mapped)
    print(
        f'Mean accepted response time: {mean_time:.2f} seconds ({mean_time/60/60/24:.2f} days).')


if __name__ == '__main__':
    start_time = datetime.now()
    # MapReduce tasks to be running with the 'posts' file:
    file_name = 'posts'
    top_dates_created_posts(file_name)
    get_answers_views_ratio(file_name)
    get_mean_time_response(file_name)
    end_time = datetime.now()
    print('Total execution time: {}'.format(end_time - start_time))
