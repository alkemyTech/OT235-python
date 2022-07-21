    #!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import mr2_d

reduce2 =[]   
class TestMr2_d(unittest.TestCase):
        # El tipo de excepcion debe ser TypeError
        # Hacemos una prueba para cada tipo que sabemos que no es compatible con el parametro
        #Vemos que no se levanta la excepcion y se obtuvo un error por cada una de las llamadas  
    def test_1(self):
        self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,'diez',True,0)
    def test_2(self):
        self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,True,True,0)
    def test_3(self):    
        self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,3.1224,True,0)
    def test_4(self):    
        self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,'boolean',0)
    def test_5(self):    
        self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,5,0)
    def test_6(self):    
        self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,4.12,0)
    def test_7(self):   
        self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,True,'cero')
    def test_8(self):    
        self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,True,False)
    def test_9(self):    
        self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,True,1+3j)
       # Esperaba que la funcion fuera instancia de alguno de los siguientes tipos de datos pero arroja error. 
    def test_10(self):
        self.assertIsInstance(mr2_d.reduce_c_args,list)
    def test_11(self):   
        self.assertIsInstance(mr2_d.reduce_c_args,dict)
    # se esperaba que arrojara error pero es instancia de lista y no de diccionario     
    def test_12(self): 
        d={}   
        self.assertRaises(TypeError,mr2_d.reducer_menores(d, 10, True,0,None))
    def test_13(self):
        p =[]    
        self.assertRaises(TypeError,mr2_d.reducer_menores(p, True, True,0,None))
    def test_14(self):
        p =[]    
        self.assertRaises(TypeError,mr2_d.reducer_menores(p, "hola", True,0,None))
    def test_14(self):
        p =[]    
        self.assertRaises(TypeError,mr2_d.reducer_menores(p, 10.90, True,0,None))
    def test_15(self):
        p =[]    
        self.assertRaises(TypeError,mr2_d.reducer_menores(p, 1+3j, True,0,None))
    def test_16(self):
        p =[]    
        self.assertRaises(TypeError,mr2_d.reducer_menores(p, True, 6,0,None))    
    def test_16(self):
        p =[]    
        self.assertRaises(TypeError,mr2_d.reducer_menores(p,10,True,"hola",0,None)) 
    def test_17(self):
        p =[]    
        self.assertRaises(TypeError,mr2_d.reducer_menores(p,10,True,3.5,0,None)) 
    def test_18(self):
        p =[]    
        self.assertRaises(TypeError,mr2_d.reducer_menores(p,10,True,3+6j,0,None)) 
    def test_19(self):
        p =[]    
        self.assertRaises(TypeError,mr2_d.reducer_menores(p,10,True,False,None))  
    def test_20(self):
        p =[]    
        self.assertRaises(TypeError,mr2_d.reducer_menores(p,10,True,0,"None"))  

        '''
        sabrina@DESKTOP-5GK77AA:~/Big Data$ python3 -m unittest test_mr2_d.py
        Relacion entre score y respuestas: en Promedio por cada respuesta corresponden 2.0 puntos
        Top 10 mas score sin respuestas aceptadas       Id Score
        0   3086    95
        1  22795    95
        2   1751    94
        3   7572    94
        4  36590    93
        5   4376    91
        6   1026     9
        7   1265     9
        8   1353     9
        9   1417     9
        Top 10 post con mayor actividad hasta respuesta correcta          Id                      dif
        2583  28416 571 days 05:08:39.020000
        1230  14056 541 days 01:16:13.664000
        113    1023 472 days 02:08:44.420000
        556    5625 471 days 03:35:11.950000
        95      879 465 days 12:48:49.040000
        129    1177 457 days 09:54:14.687000
        550    5551 440 days 13:24:02.323000
        945   10551 436 days 12:20:54.006000
        282    2651 434 days 02:57:53.613000
        1384  15661 406 days 13:29:23.780000
        Tiempo de ejecucion--- 29.044339179992676 seconds ---
        EFFEEEEEEEEE
        ======================================================================
        ERROR: test_1 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 14, in test_1
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,'diez',True,0)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_12 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 38, in test_12
            self.assertRaises(TypeError,mr2_d.reducer_menores(d, 10, True,0,None))
        File "/home/sabrina/Big Data/mr2_d.py", line 32, in reducer_menores
            if isinstance(p[0],list):
        KeyError: 0

        ======================================================================
        ERROR: test_2 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 16, in test_2
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,True,True,0)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_3 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 18, in test_3
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,3.1224,True,0)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_4 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 20, in test_4
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,'boolean',0)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_5 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 22, in test_5
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,5,0)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_6 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 24, in test_6
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,4.12,0)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_7 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 26, in test_7
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,True,'cero')
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_8 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 28, in test_8
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,True,False)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_9 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 30, in test_9
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,True,1+3j)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        FAIL: test_10 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 33, in test_10
            self.assertIsInstance(mr2_d.reduce_c_args,list)
        AssertionError: <function reduce_c_args at 0x7fa9675385e0> is not an instance of <class 'list'>

        ======================================================================
        FAIL: test_11 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 35, in test_11
            self.assertIsInstance(mr2_d.reduce_c_args,dict)
        AssertionError: <function reduce_c_args at 0x7fa9675385e0> is not an instance of <class 'dict'>

        ----------------------------------------------------------------------
        Ran 12 tests in 0.004s

        FAILED (failures=2, errors=10)
        sabrina@DESKTOP-5GK77AA:~/Big Data$ python3 -m unittest test_mr2_d.py
        Relacion entre score y respuestas: en Promedio por cada respuesta corresponden 2.0 puntos
        Top 10 mas score sin respuestas aceptadas       Id Score
        0   3086    95
        1  22795    95
        2   1751    94
        3   7572    94
        4  36590    93
        5   4376    91
        6   1026     9
        7   1265     9
        8   1353     9
        9   1417     9
        Top 10 post con mayor actividad hasta respuesta correcta          Id                      dif
        2583  28416 571 days 05:08:39.020000
        1230  14056 541 days 01:16:13.664000
        113    1023 472 days 02:08:44.420000
        556    5625 471 days 03:35:11.950000
        95      879 465 days 12:48:49.040000
        129    1177 457 days 09:54:14.687000
        550    5551 440 days 13:24:02.323000
        945   10551 436 days 12:20:54.006000
        282    2651 434 days 02:57:53.613000
        1384  15661 406 days 13:29:23.780000
        Tiempo de ejecucion--- 29.337819576263428 seconds ---
        EFFEEEEEEEEEEEEEEEEE
        ======================================================================
        ERROR: test_1 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 14, in test_1
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,'diez',True,0)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_12 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 39, in test_12
            self.assertRaises(TypeError,mr2_d.reducer_menores(d, 10, True,0,None))
        File "/home/sabrina/Big Data/mr2_d.py", line 32, in reducer_menores
            if isinstance(p[0],list):
        KeyError: 0

        ======================================================================
        ERROR: test_13 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 42, in test_13
            self.assertRaises(TypeError,mr2_d.reducer_menores(p, True, True,0,None))
        File "/home/sabrina/Big Data/mr2_d.py", line 32, in reducer_menores
            if isinstance(p[0],list):
        IndexError: list index out of range

        ======================================================================
        ERROR: test_14 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 48, in test_14
            self.assertRaises(TypeError,mr2_d.reducer_menores(p, 10.90, True,0,None))
        File "/home/sabrina/Big Data/mr2_d.py", line 32, in reducer_menores
            if isinstance(p[0],list):
        IndexError: list index out of range

        ======================================================================
        ERROR: test_15 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 51, in test_15
            self.assertRaises(TypeError,mr2_d.reducer_menores(p, 1+3j, True,0,None))
        File "/home/sabrina/Big Data/mr2_d.py", line 32, in reducer_menores
            if isinstance(p[0],list):
        IndexError: list index out of range

        ======================================================================
        ERROR: test_16 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 57, in test_16
            self.assertRaises(TypeError,mr2_d.reducer_menores(p,10,True,"hola",0,None))
        TypeError: reducer_menores() takes 5 positional arguments but 6 were given

        ======================================================================
        ERROR: test_17 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 60, in test_17
            self.assertRaises(TypeError,mr2_d.reducer_menores(p,10,True,3.5,0,None))
        TypeError: reducer_menores() takes 5 positional arguments but 6 were given

        ======================================================================
        ERROR: test_18 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 63, in test_18
            self.assertRaises(TypeError,mr2_d.reducer_menores(p,10,True,3+6j,0,None))
        TypeError: reducer_menores() takes 5 positional arguments but 6 were given

        ======================================================================
        ERROR: test_19 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 66, in test_19
            self.assertRaises(TypeError,mr2_d.reducer_menores(p,10,True,False,None))
        File "/home/sabrina/Big Data/mr2_d.py", line 32, in reducer_menores
            if isinstance(p[0],list):
        IndexError: list index out of range

        ======================================================================
        ERROR: test_2 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 16, in test_2
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,True,True,0)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_20 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 69, in test_20
            self.assertRaises(TypeError,mr2_d.reducer_menores(p,10,True,0,"None"))
        File "/home/sabrina/Big Data/mr2_d.py", line 32, in reducer_menores
            if isinstance(p[0],list):
        IndexError: list index out of range

        ======================================================================
        ERROR: test_3 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 18, in test_3
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,3.1224,True,0)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_4 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 20, in test_4
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,'boolean',0)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_5 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 22, in test_5
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,5,0)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_6 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 24, in test_6
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,4.12,0)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_7 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 26, in test_7
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,True,'cero')
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_8 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 28, in test_8
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,True,False)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        ERROR: test_9 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 30, in test_9
            self.assertRaises(TypeError,mr2_d.reduce_c_args,mr2_d.reducer_menores,reduce2,10,True,1+3j)
        File "/usr/lib/python3.8/unittest/case.py", line 816, in assertRaises
            return context.handle('assertRaises', args, kwargs)
        File "/usr/lib/python3.8/unittest/case.py", line 202, in handle
            callable_obj(*args, **kwargs)
        File "/home/sabrina/Big Data/mr2_d.py", line 23, in reduce_c_args
            value = next(it)
        StopIteration

        ======================================================================
        FAIL: test_10 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 33, in test_10
            self.assertIsInstance(mr2_d.reduce_c_args,list)
        AssertionError: <function reduce_c_args at 0x7f67f5bf8550> is not an instance of <class 'list'>

        ======================================================================
        FAIL: test_11 (test_mr2_d.TestMr2_d)
        ----------------------------------------------------------------------
        Traceback (most recent call last):
        File "/home/sabrina/Big Data/test_mr2_d.py", line 35, in test_11
            self.assertIsInstance(mr2_d.reduce_c_args,dict)
        AssertionError: <function reduce_c_args at 0x7f67f5bf8550> is not an instance of <class 'dict'>

        ----------------------------------------------------------------------
        Ran 20 tests in 0.006s

        FAILED (failures=2, errors=18)  
        '''         

if __name__ == "__main__":
        unittest.main()