import unittest
target = __import__ ('map_reduce')
#import calc

class Test_text_cleaner(unittest.TestCase):

	def test_text_cleaner(self):
		"""
		Elimina caracteres especiales de un texto

		Testeo si funciona con una cadena de texto 
		>>> target_text_cleaner('Sera mayor > o sera menor< sera distinto <> o - que eso')
		'Sera mayor  o sera menor sera distinto  o   que eso'
		
		Testeo si funciona con una caracteres especiales texto
		>>>target_text_cleaner('Sera 4$? > 5%')
		'Sera 4$?  5%'

		Testeo si funciona sin nada que reemplazar
		>>>target_text_cleaner('4')
		'4'

		"""
		datos='Sera mayor > o sera menor< sera distinto <> o - que eso'
		resultado=target.text_cleaner(datos)
		limpio='Sera mayor  o sera menor sera distinto  o   que eso'
		self.assertEqual(resultado,limpio)
		datos='Sera 4$? > 5%'
		resultado=target.text_cleaner(datos)
		limpio='Sera 4$?  5%'
		self.assertEqual(resultado,limpio)
		datos='4'
		resultado=target.text_cleaner(datos)
		limpio='4'
		self.assertEqual(resultado,limpio)

	def test_seconds_to_hours(self):
		"""
		Convierte segundos a horas minutos segundos
		:parametros: segundos en int
		:devuelve: devuelve caracteres horas: horas:minutos:segundos
		
		Testeo con 60 segundos
		>>>target.seconds_to_hours(60)
		'horas: 0:1:0'

		Testeo con 0 segundos
		>>>target.seconds_to_hours(0)
		'horas: 0:0:0'

		Testeo con 900001 segundos
		>>>target.seconds_to_hours(900001)
		'horas: 250:0:1'		

		"""

		datos=60
		resultado=target.seconds_to_hours(datos)
		limpio='horas: 0:1:0'
		self.assertEqual(resultado,limpio)

		datos=0
		resultado=target.seconds_to_hours(datos)
		limpio='horas: 0:0:0'
		self.assertEqual(resultado,limpio)

		datos=900001
		resultado=target.seconds_to_hours(datos)
		limpio='horas: 250:0:1'
		self.assertEqual(resultado,limpio)

	def test_map_xml(self):

		"""
	    Lee un archivo xml y devuelve una lista de valores.
	    Valores devueltos:
	    [Tags(str), AnswerCount(int)]
	    [Body(str), Score(int)]
	    [Id(int), CreationDate(datetime)]
	    [ParentId(int), CreationDate(datetime)]

    	:return: Lista de valores
		
		Se creo un lote de prueba posts_test.xml esta hardcoded en el scipt
    	>>>target.map_xml()
    	[['tags: <discussion><status-completed><uservoice>', 13], ['body: <p>Now that we have meta.stackoverflow.com, should we continue using uservoice.com?</p>\n\n<p>The only requirement for participation here is that you have an existing stackoverflow / serverfault / superuser account -- but you can be a brand new user, so anonymous participation is allowed.</p>\n\n<p>It seems that questions tagged "bug" or "feature" could be voted on and commented in a fashion very similar to what uservoice already offers.</p>\n\n<p>Some people wanted to <a href="http://stackoverflow.uservoice.com/pages/1722-general/suggestions/193243-move-from-uv-to-getsatisfaction" rel="nofollow">move to GetSatisfaction</a>, but I wasn\'t happy with that service.</p>\n', 57], ["body: <p>As much as UV annoys me (and believe me it does), this doesn't do the job of UV.</p>\n\n<p>The key part of UV is the issue tracking component. Where is the part on here to say what's been declined, started, completed, etc?</p>\n\n<p>This could easily be the place for questions like about SO and how it works though (ie things that don't necessarily result in site changes but are just issues people want to discuss).</p>\n", 23], ['post: 2009-06-28 07:14:29', 'answer: 2009-06-28 08:14:46'], ['body: <p>When using Google for your OpenId provider, it generates a different openid url for each website you use with it.  This means that for stackoverflow.com, meta.stackoverflow.com, superuser.com and serverfault.com you will have 4 different openids.</p>\n\n<p>Currently you can have an main and an alternate openid - should the system support as moany openids as there are sites in the stackoverflow family?</p>\n', 5], ["body: <p>I'd prefer to see uservoice.com replaced with this site and expect that to naturally happen. The end-user experience is consistent and the friction of working with the UV site is removed.</p>\n\n<p>meta.stackoverflow.com won't have every feature to assist this initially but these can be added.</p>\n", 52],['post: 2009-06-28 07:14:29', 'answer: 2009-06-28 08:50:44']]
    	
    	Fallo detectado no guarda bien el idi se borra en cada lectura de post 1

    	"""
		resultado=target.map_xml()
		limpio=[['tags: <discussion><status-completed><uservoice>', 13], ['body: <p>Now that we have meta.stackoverflow.com, should we continue using uservoice.com?</p>\n\n<p>The only requirement for participation here is that you have an existing stackoverflow / serverfault / superuser account -- but you can be a brand new user, so anonymous participation is allowed.</p>\n\n<p>It seems that questions tagged "bug" or "feature" could be voted on and commented in a fashion very similar to what uservoice already offers.</p>\n\n<p>Some people wanted to <a href="http://stackoverflow.uservoice.com/pages/1722-general/suggestions/193243-move-from-uv-to-getsatisfaction" rel="nofollow">move to GetSatisfaction</a>, but I wasn\'t happy with that service.</p>\n', 57], ["body: <p>As much as UV annoys me (and believe me it does), this doesn't do the job of UV.</p>\n\n<p>The key part of UV is the issue tracking component. Where is the part on here to say what's been declined, started, completed, etc?</p>\n\n<p>This could easily be the place for questions like about SO and how it works though (ie things that don't necessarily result in site changes but are just issues people want to discuss).</p>\n", 23], ['post: 2009-06-28 07:14:29', 'answer: 2009-06-28 08:14:46'], ['body: <p>When using Google for your OpenId provider, it generates a different openid url for each website you use with it.  This means that for stackoverflow.com, meta.stackoverflow.com, superuser.com and serverfault.com you will have 4 different openids.</p>\n\n<p>Currently you can have an main and an alternate openid - should the system support as moany openids as there are sites in the stackoverflow family?</p>\n', 5], ["body: <p>I'd prefer to see uservoice.com replaced with this site and expect that to naturally happen. The end-user experience is consistent and the friction of working with the UV site is removed.</p>\n\n<p>meta.stackoverflow.com won't have every feature to assist this initially but these can be added.</p>\n", 52],['post: 2009-06-28 07:14:29', 'answer: 2009-06-28 08:50:44']]
		self.assertEqual(resultado,limpio)

	def test_reduce_top(self):
	    """
	    Devuelve las top n listas de tags con respuestas aceptadas
	    :parametros: lista mapped
	    :devuelve: lista con top n
	    datos=[['tags: <discussion><status-completed><uservoice>', 13], ['body: <p>Now that we have meta.stackoverflow.com, should we continue using uservoice.com?</p>\n\n<p>The only requirement for participation here is that you have an existing stackoverflow / serverfault / superuser account -- but you can be a brand new user, so anonymous participation is allowed.</p>\n\n<p>It seems that questions tagged "bug" or "feature" could be voted on and commented in a fashion very similar to what uservoice already offers.</p>\n\n<p>Some people wanted to <a href="http://stackoverflow.uservoice.com/pages/1722-general/suggestions/193243-move-from-uv-to-getsatisfaction" rel="nofollow">move to GetSatisfaction</a>, but I wasn\'t happy with that service.</p>\n', 57], ["body: <p>As much as UV annoys me (and believe me it does), this doesn't do the job of UV.</p>\n\n<p>The key part of UV is the issue tracking component. Where is the part on here to say what's been declined, started, completed, etc?</p>\n\n<p>This could easily be the place for questions like about SO and how it works though (ie things that don't necessarily result in site changes but are just issues people want to discuss).</p>\n", 23], ['post: 2009-06-28 07:14:29', 'answer: 2009-06-28 08:14:46'], ['body: <p>When using Google for your OpenId provider, it generates a different openid url for each website you use with it.  This means that for stackoverflow.com, meta.stackoverflow.com, superuser.com and serverfault.com you will have 4 different openids.</p>\n\n<p>Currently you can have an main and an alternate openid - should the system support as moany openids as there are sites in the stackoverflow family?</p>\n', 5], ["body: <p>I'd prefer to see uservoice.com replaced with this site and expect that to naturally happen. The end-user experience is consistent and the friction of working with the UV site is removed.</p>\n\n<p>meta.stackoverflow.com won't have every feature to assist this initially but these can be added.</p>\n", 52],['post: 2009-06-28 07:14:29', 'answer: 2009-06-28 08:50:44']]
	    >>>target.reduce_top(datos)
	    [['Respuestas aceptadas: 13', 'tags: discussion status completed uservoice']]
		
		datos=[['tags: <discussion><status-completed><uservoice>', 13],['tags: mas de 10', 14],
	    ['tags: mas de 10', 16],['tags: a', 17],['tags: b', 11],
	    ['tags: mas de 10', 10],['tags: a', 9],['tags: b', 19],
	    ['tags: mas de 10', 18],['tags: a', 11],['tags: b', 20]]
	    >>>target.reduce_top(datos)
	    [['Respuestas aceptadas: 20', 'tags: b'],
	    ['Respuestas aceptadas: 18', 'tags: mas de 10'],
	    ['Respuestas aceptadas: 17', 'tags: a'],
	    ['Respuestas aceptadas: 13', 'tags: discussion status completed uservoice']]
	    
	    Fallo detectado no ordena correctamente de mayor a menor y repite tags 

	    """
	    datos=[['tags: <discussion><status-completed><uservoice>', 13], ['body: <p>Now that we have meta.stackoverflow.com, should we continue using uservoice.com?</p>\n\n<p>The only requirement for participation here is that you have an existing stackoverflow / serverfault / superuser account -- but you can be a brand new user, so anonymous participation is allowed.</p>\n\n<p>It seems that questions tagged "bug" or "feature" could be voted on and commented in a fashion very similar to what uservoice already offers.</p>\n\n<p>Some people wanted to <a href="http://stackoverflow.uservoice.com/pages/1722-general/suggestions/193243-move-from-uv-to-getsatisfaction" rel="nofollow">move to GetSatisfaction</a>, but I wasn\'t happy with that service.</p>\n', 57], ["body: <p>As much as UV annoys me (and believe me it does), this doesn't do the job of UV.</p>\n\n<p>The key part of UV is the issue tracking component. Where is the part on here to say what's been declined, started, completed, etc?</p>\n\n<p>This could easily be the place for questions like about SO and how it works though (ie things that don't necessarily result in site changes but are just issues people want to discuss).</p>\n", 23], ['post: 2009-06-28 07:14:29', 'answer: 2009-06-28 08:14:46'], ['body: <p>When using Google for your OpenId provider, it generates a different openid url for each website you use with it.  This means that for stackoverflow.com, meta.stackoverflow.com, superuser.com and serverfault.com you will have 4 different openids.</p>\n\n<p>Currently you can have an main and an alternate openid - should the system support as moany openids as there are sites in the stackoverflow family?</p>\n', 5], ["body: <p>I'd prefer to see uservoice.com replaced with this site and expect that to naturally happen. The end-user experience is consistent and the friction of working with the UV site is removed.</p>\n\n<p>meta.stackoverflow.com won't have every feature to assist this initially but these can be added.</p>\n", 52],['post: 2009-06-28 07:14:29', 'answer: 2009-06-28 08:50:44']]
	    resultado=target.reduce_top(datos)
	    limpio=[['Respuestas aceptadas: 13', 'tags: discussion status completed uservoice']]
	    self.assertEqual(resultado,limpio)

	    datos=[['tags: <discussion><status-completed><uservoice>', 13],['tags: mas de 10', 14],
	    ['tags: mas de 10', 16],['tags: a', 17],['tags: b', 11],
	    ['tags: mas de 10', 10],['tags: a', 9],['tags: b', 19],
	    ['tags: mas de 10', 18],['tags: a', 11],['tags: b', 20]]
	    resultado=target.reduce_top(datos)
	    limpio=[['Respuestas aceptadas: 20', 'tags: b'],
	    ['Respuestas aceptadas: 18', 'tags: mas de 10'],
	    ['Respuestas aceptadas: 17', 'tags: a'],
	    ['Respuestas aceptadas: 13', 'tags: discussion status completed uservoice']]
	    self.assertEqual(resultado,limpio)

	def test_reduce_score(self):
		"""
		Devuelve el numero promedio de palabras por post y el promedio de score por post
		:parametro: lista mapped
		:devuelve: lista con promedio palabras por post y promedio score por post

		datos=[['tags: <discussion><status-completed><uservoice>', 13], ['body: <p>1']]
		>>>target.reduce_score(datos)
		[1.0, 57.0]

		Fallo cuenta la palabra body.
		"""
		datos= [['tags: <discussion><status-completed><uservoice>', 13], ['body: a', 57]]
		resultado=target.reduce_score(datos)
		limpio=[[1.0, 57.0]]
		self.assertEqual(resultado,limpio)


	def test_reduce_answers(self):
		"""
		Calcula la demora promedio de respuesta
    	:parametros: lista mapped
    	:devuelve: promedio en horas
		datos= [['post: 2009-06-28 07:50:00', 'answer: 2009-06-28 08:50:00'],['post: 2009-06-28 02:50:00', 'answer: 2009-06-28 09:50:00'],['post: 2009-06-28 07:50:00', 'answer: 2009-06-28 08:50:00']]
		>>>target.reduce_answers(datos)
		'horas: 3:0:0'
		
    	Falla el calculo de demora promedio si hay dos casos con las mismas caracteristicas

		"""
		datos= [['post: 2009-06-28 07:50:00', 'answer: 2009-06-28 08:50:00'],['post: 2009-06-28 02:50:00', 'answer: 2009-06-28 09:50:00'],['post: 2009-06-29 07:50:00', 'answer: 2009-06-29 08:50:00']]
		resultado=target.reduce_answers(datos)
		limpio='horas: 3:0:0'
		self.assertEqual(resultado,limpio)

		datos= [['post: 2009-06-28 07:50:00', 'answer: 2009-06-28 08:50:00'],['post: 2009-06-28 02:50:00', 'answer: 2009-06-28 09:50:00'],['post: 2009-06-28 07:50:00', 'answer: 2009-06-28 08:50:00']]
		resultado=target.reduce_answers(datos)
		limpio='horas: 3:0:0'
		self.assertEqual(resultado,limpio)


if __name__ == '__main__':
	unittest.main()