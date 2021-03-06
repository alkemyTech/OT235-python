SELECT universidades AS university,
	   carreras AS career,
	   fechas_de_inscripcion AS inscription_date,
	   SUBSTRING(nombres from 1 for (POSITION('-' in nombres) - 1)) AS first_name,
	   SUBSTRING(nombres from (POSITION('-' in nombres) + 1) for LENGTH(nombres)) AS last_name,
	   sexo AS gender,
	   EXTRACT(YEAR FROM age(TIMESTAMP 'NOW()',TO_DATE(fechas_nacimiento,'yy-Mon-dd'))) AS age,
	   codigos_postales AS postal_code,
	   direcciones AS "location",
	   emails AS email
FROM uba_kenedy 
WHERE universidades='universidad-j.-f.-kennedy'
AND to_date(fechas_de_inscripcion, 'YY-Mon-DD') 
BETWEEN to_date('2020-Sep-01', 'YY-Mon-DD') 
AND to_date('2021-Feb-01', 'YY-Mon-DD');