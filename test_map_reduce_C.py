import unittest
from mapReduce_Sabrina_3 import add_reducer, tag_mapper, calculate_top_10, tags_accepted_answers, task
from mapReduce_Sabrina_3 import user_mapped, user_reduce, calcule_top_10_users, relation_mapped, relation_reducer
import xml.etree.ElementTree as ET
import mock

# reading correct data file located in local folder
pathXml = 'posts.xml'
tree = ET.parse(pathXml)
root = tree.getroot()


class test_map_reduce_task_1(unittest.TestCase):
    ''' Test class for the task:
        > Top 10 tipo de post con mayor respuestas aceptadas
    '''

    def test_tag_mapper_return_type(self):
        ''' Checking the return type of tag_mapper.
            Test Result:
                Pass
        '''
        self.assertIsInstance(tag_mapper(root), dict)

    def test_tag_mapper_wrong_data(self):
        '''Use a file with different xml structure
           to check if the function returns an empty dictionary.
           Test Result:
                Pass
        '''
        pathXml_2 = 'users.xml'
        tree_2 = ET.parse(pathXml_2)
        root_2 = tree_2.getroot()
        self.assertEqual(tag_mapper(root_2), {})

    def test_tag_mapper_count(self):
        '''Use a file with known data to check the correct output
           of the function tag_mapper that counts tags types.
           Test Result:
                Pass
        '''
        # loading of xml file and data
        pathXml_3 = 'posts_test.xml'
        tree_3 = ET.parse(pathXml_3)
        root_3 = tree_3.getroot()
        # definition of the expected tag types and their counting that must
        # return the tested function
        dict_out = {'discussion': 4,
                    'status-completed': 4,
                    'uservoice': 4
                    }

        self.assertEqual(tag_mapper(root_3), dict_out)

    def test_tags_accepted_answers_attrib(self):
        '''Test over the tags_accepted_answers function with an
           object with PostTypeId atribute different that 1
           to check the return type.
           Test Result:
                Pass
        '''
        # test object creation and atribute assignation
        obj = mock.Mock()
        obj.PostTypeId = 5

        self.assertIsNone(tags_accepted_answers(obj))

    def test_tags_accepted_answers_attrib_err(self):
        '''Check the Exception raising when an object
           has a wrong atribute name.
           Test Result:
                Fail (function does not raises Exception)
        '''
        # test object creation and atribute assignation
        obj = mock.Mock()
        obj.PostTypeId = "1"
        obj.AcceptedAnswerWrongAttrib = "2"

        with self.assertRaises(Exception):
            tags_accepted_answers(obj)

    def test_add_reducer(self):
        '''Check the add_reducer function with a known dictionary
           of tag types and their counting.
           Output dictionary must sum value of each key when key names
           are repeated.
           Test Result:
                Fail
        '''
        # input dictionary
        mock_dict = {'category_1': 100,
                     'category_2': 5,
                     'category_1': 20
                     }
        # expected output dictionary
        mock_dict_recuced = {'category_1': 120,
                             'category_2': 5
                             }

        self.assertEqual(add_reducer(mock_dict), mock_dict_recuced)

    def test_calculate_top_10(self):
        '''For a dictionary with integer values,
           check the top 10 items acording to their values.
           Test Result:
                Pass
        '''
        # input dictionary
        mock_dict = {'category_1': 120,
                     'category_2': 119,
                     'category_3': 119,
                     'category_4': 100,
                     'category_5': 90,
                     'category_6': 80,
                     'category_7': 70,
                     'category_8': 60,
                     'category_9': 50,
                     'category_10': 45,
                     'category_11': 40
                     }
        # expected output dictionary
        mock_dict_out = {'category_1': 120,
                         'category_2': 119,
                         'category_3': 119,
                         'category_4': 100,
                         'category_5': 90,
                         'category_6': 80,
                         'category_7': 70,
                         'category_8': 60,
                         'category_9': 50,
                         'category_10': 45,
                         }

        self.assertEqual(calculate_top_10(mock_dict), mock_dict_out)

    def test_task_wrong_number(self):
        '''Check the behaviour of task function when
            the parameter has a wrong task number.
        Test Result:
                Fail (function does not raises Exception)
        '''
        task_parameter_1 = 5

        with self.assertRaises(Exception):
            task(task_parameter_1)


class test_map_reduce_task_2(unittest.TestCase):
    ''' Test class for the task:
        > Top 10 de usuarios con mayor porcentaje de respuestas favoritas
    '''

    def test_user_mapped_wrong_path(self):
        ''' Check the exception raising in user_mapped function
            when the parameter is a non-existing file.
            Test Result:
                Fail
        '''
        with self.assertRaises(Exception):
            file = 'dummy_file.xml'
            user_mapped(file)

    def test_user_mapped_return_non_empty_list(self):
        ''' Check a non zero length of the return list in user_mapped
            function, when the file path is the correct one.
            Test Result:
                Pass
        '''
        self.assertTrue(len(user_mapped(pathXml)) != 0)

    def test_user_reduce_expected_return(self):
        '''Check the functionality of the user_reduce function with
            a given input parameter and an expected return element.
            Test Result:
                Fail
        '''
        # Non reduced dict to use as input
        dict_input = [{'owner_id_1': 10,
                      'owner_id_2': 35,
                      'owner_id_1': 3
                      }]
        # Reduced dict to use as expected output. For identical keys in
        # dict_input, values are sumed. 
        dict_expected = {'owner_id_1': 13,
                         'owner_id_2': 35,
                         }

        self.assertEqual(user_reduce(dict_input), dict_expected)

    def test_calcule_top_10_users(self):
        ''' Check the calcule_top_10_users function that sorts
            a dictionary by its values and return a sorted list of tuples
            Test Result:
                Pass
        '''
        # input dictionary unsorted
        mock_dict = {'category_1': 120,
                     'category_10': 45,
                     'category_2': 119,
                     'category_3': 119,
                     'category_4': 100,
                     'category_5': 90,
                     'category_6': 80,
                     'category_7': 70,
                     'category_8': 60,
                     'category_9': 50,
                     'category_11': 40
                     }
        # expected output list of tuples sorted by second elements.
        mock_dict_out = [('category_1', 120),
                         ('category_2', 119),
                         ('category_3', 119),
                         ('category_4', 100),
                         ('category_5', 90),
                         ('category_6', 80),
                         ('category_7', 70),
                         ('category_8', 60),
                         ('category_9', 50),
                         ('category_10', 45),
                         ('category_11', 40)
                         ]

        self.assertEqual(calcule_top_10_users(mock_dict), mock_dict_out)


class test_map_reduce_task_3(unittest.TestCase):
    '''Test class for the task:
        > Relación entre cantidad de palabras en un post y su cantidad de respuestas
    '''

    def test_relation_mapped_expected_output(self):
        ''' Check the relation_mapped function with a known file
            with posts of known number of words and known answer counts.
            Test Result:
                Fail
        '''
        # File with known data
        pathXml = 'posts_test.xml'
        # Expected output for pathXml data
        expected_count = [{2: 13},
                          {3: 2},
                          {2: 50},
                          {1: 10}
                          ]

        self.assertEqual(relation_mapped(pathXml), expected_count)

    def test_relation_reducer_empty_list(self):
        '''Function relation_reducer tested with an empty 
           parameter, expected to return an empty dictionary.
           Test Result:
                Pass
        '''
        empty_list = []

        self.assertEqual(relation_reducer(empty_list), {})


if __name__ == "__main__":
    unittest.main()
