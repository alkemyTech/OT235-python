SELECT universidad AS university,
       carrera AS career,
       fecha_de_inscripcion AS inscription_date,
       SUBSTRING(name, 1, POSITION(' ' IN name)) AS first_name,
       SUBSTRING(name FROM POSITION(' ' IN name)) AS last_name,
       sexo AS sex,
       fecha_nacimiento AS birth_date,
       codigo_postal AS zip_code,
       correo_electronico AS email,
	   date_part('year',now())-date_part('year',to_date(fecha_nacimiento, 'YY-MM-DD')) as age
FROM public.flores_comahue
WHERE universidad = 'UNIV. NACIONAL DEL COMAHUE'
AND fecha_de_inscripcion BETWEEN '2020-09-01' AND '2021-02-01'

