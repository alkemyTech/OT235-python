from stack_overflow_map_reduce_E import top_dates_created_posts
from stack_overflow_map_reduce_E import get_answers_views_ratio
from stack_overflow_map_reduce_E import get_mean_time_response

"""
For this test, only the first 100 pieces of data from the original file are used.
error generales:

general errors:
    1)  It generates an error in all the functions if it is executed 
        from another folder and not the root.

    2)  The script if the file logging_Big_Data.py and
        log_Big_Data.cfg is not found produces a general error that does not allow 
        the execution of the functions.
"""

"""
******************************************************
** unit test of the function top_dates_created_posts **
******************************************************
"""

#Unit test with original post without any modifications (first 100 data)
def test_top_dates_created_posts_original():
    """
    what was done:
        1) The post_test key was added to the file dictionary to the original function to perform the tests

    """
    file_name = 'posts'
    file_name2 = 'posts_test'
    assert top_dates_created_posts(file_name) == top_dates_created_posts(file_name2)

    """
    result:
        1) The function is not executed if the test is applied from another path than its folder.
        example: running pytest from /home/cris will give a missing files error.
        solution: run pytest from the root folder in my case (OT235-176-184).
        Images in folder: top_dates_created_posts_original
    """



#unit test with repeated elements
def test_top_dates_created_posts_rep():
    """
     what was done:
        1) The first 10 data are added repeatedly to the end of the file to observe its result
    """
    file_name = 'posts'
    file_name2 = 'posts_test_rep'
    assert top_dates_created_posts(file_name) == top_dates_created_posts(file_name2)

    """
    result:
        1) This test does not represent any problem for the function
        Images in folder: top_dates_created_posts_rep
    """



#We will verify that adding files with PostTypeId = 3 does not pose any problem for the function
def test_top_dates_created_posts_PostTypeId3():
    """
    what was done:
        1) Added 10 elements with PostTypeID=3 to the end of the file
    """
    file_name = 'posts'
    file_name2 = 'posts_test_PostTypeId'
    assert top_dates_created_posts(file_name) == top_dates_created_posts(file_name2)

    """
    result:
        1) It does not present any problem.
        Images in folder: test_top_dates_created_posts_PostTypeId3
    """



#the ID of the first 3 values is modified to see its interaction with data in int
def test_top_dates_created_posts_idint():
    """
    what was done:
        1)  Changed the first 3 values of the test file elements from str to int
    """
    file_name = 'posts'
    file_name2 = 'posts_test_Id_int'
    assert top_dates_created_posts(file_name) == top_dates_created_posts(file_name2)

    """
    result:
        1) Generate a format error
        Images in folder: test_top_dates_created_posts_idint
    """




"""
******************************************************
** unit test of the function get_answers_views_ratio **
******************************************************
"""

#Unit test with original post without any modifications (first 100 data)
def test_get_answers_views_ratio_original():
    """
    what was done:
        1) The post_test key was added to the file dictionary to the original function to perform the tests

    """
    file_name = 'posts'
    file_name2 = 'posts_test'
    assert get_answers_views_ratio(file_name) == get_answers_views_ratio(file_name2)

    """
    result:
        1) It does not present any problem.
        Except for the same route problem that the previous function presented
        Images in folder: test_get_answers_views_ratio_original
    """



#unit test with repeated elements
def test_get_answers_views_ratio_rep():
    """
     what was done:
        1) The first 10 data are added repeatedly to the end of the file to observe its result
    """
    file_name = 'posts'
    file_name2 = 'posts_test_rep'
    assert get_answers_views_ratio(file_name) == get_answers_views_ratio(file_name2)

    """
    result:
        1) This test does not represent any problem for the function
        Images in folder: test_get_answers_views_ratio_rep
    """



#We will verify that adding files with PostTypeId = 3 does not pose any problem for the function
def test_get_answers_views_ratio_PostTypeId3():
    """
        what was done:
            1) Added 10 elements with PostTypeID=3 to the end of the file
    """
    file_name = 'posts'
    file_name2 = 'posts_test_PostTypeId'
    assert get_answers_views_ratio(file_name) == get_answers_views_ratio(file_name2)
    """
    result:
        1) It does not present any problem.
        Images in folder: test_get_answers_views_ratio_PostTypeId3
    """


#the ID of the first 3 values is modified to see its interaction with data in int
def test_get_answers_views_ratio_idint():
    """
    what was done:
        1)  Changed the first 3 values of the test file elements from str to int
    """
    file_name = 'posts'
    file_name2 = 'posts_test_Id_int'
    assert get_answers_views_ratio(file_name) == get_answers_views_ratio(file_name2)

    """
    result:
        1) Generate a format error
        Images in folder: test_top_dates_created_posts_idint
    """


"""
******************************************************
** unit test of the function get_mean_time_response **
******************************************************
"""

#Unit test with original post without any modifications (first 100 data)
def test_get_mean_time_response_original():
    """
    what was done:
        1) The post_test key was added to the file dictionary to the original function to perform the tests
    """
    file_name = 'posts'
    file_name2 = 'posts_test'
    assert get_mean_time_response(file_name) == get_mean_time_response(file_name2)

    """
    result:
        1) Like all the previous functions, it generates an error if it is executed from another root directory.
        Images in folder: test_get_mean_time_response_original
    """



#unit test with repeated elements
def test_get_mean_time_response_rep():
    """
     what was done:
        1) The first 10 data are added repeatedly to the end of the file to observe its result
    """
    file_name = 'posts'
    file_name2 = 'posts_test_rep'
    assert get_mean_time_response(file_name) == get_mean_time_response(file_name2)

    """
    result:
        1) This test does not represent any problem for the function
        Images in folder: get_mean_time_response_rep
    """



#We will verify that adding files with PostTypeId = 3 does not pose any problem for the function
def test_get_mean_time_response_PostTypeID():
    """
    what was done:
        1) Added 10 elements with PostTypeID=3 to the end of the file
    """
    file_name = 'posts'
    file_name2 = 'posts_test_PostTypeId'
    assert get_mean_time_response(file_name) == get_mean_time_response(file_name2)

    """
    result:
        1) It does not present any problem.
        Images in folder: test_get_mean_time_response_PostTypeID
    """

#the ID of the first 3 values is modified to see its interaction with data in int
def test_get_mean_time_response_idint():
    """
    what was done:
        1)  Changed the first 3 values of the test file elements from str to int
    """
    file_name = 'posts'
    file_name2 = 'posts_test_Id_int'
    assert get_mean_time_response(file_name) == get_mean_time_response(file_name2)

    """
    result:
        1) Generate a format error
        Images in folder: test_get_mean_time_response_idint
    """