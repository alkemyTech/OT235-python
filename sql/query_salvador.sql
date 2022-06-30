SELECT universidad AS university,
       carrera AS career,
       fecha_de_inscripcion AS inscription_date,
       SUBSTRING(nombre, 1, POSITION('_' IN nombre)) AS first_name,
       SUBSTRING(nombre FROM POSITION('_' IN nombre)) AS last_name,
       sexo AS sex,
       TO_DATE(fecha_nacimiento,'DD-Mon-YY') AS birth_date,
       localidad AS location,
       email,
	   date_part('year',now())-date_part('year',to_date(fecha_nacimiento, 'DD-Mon-YY')) as age
FROM public.salvador_villa_maria
WHERE universidad = 'UNIVERSIDAD_DEL_SALVADOR'
AND TO_DATE(fecha_de_inscripcion,'DD-Mon-YY') BETWEEN '01-Sep-20' AND '01-Feb-21'

