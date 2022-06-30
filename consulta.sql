SELECT  universities,
        careers,
        inscription_dates AS inscription_date,
	    SPLIT_PART("names",'-',2) AS last_name,
	    SPLIT_PART("names",'-',1) AS first_name,
        sexo AS gender,
	    EXTRACT(YEAR FROM age(TIMESTAMP 'NOW()',TO_DATE(birth_dates,'DD-MM-YYYY'))) AS age,
	    locations AS "location",
        emails AS email
FROM lat_sociales_cine 
WHERE universities LIKE '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES'
AND inscription_dates BETWEEN '01-02-2021' AND '01-09-2020';

SELECT  universidades as universities,
        carreras as careers,
        fechas_de_inscripcion AS inscription_date,
	    SPLIT_PART(nombres,'-',2) AS last_name,
	    SPLIT_PART(nombres,'-',1) AS first_name,
        sexo AS gender,
 	    fechas_nacimiento,
-- 	    locations AS "location",
		codigos_postales as postal_code,
        emails AS email
FROM uba_kenedy 
WHERE universidades LIKE 'universidad-j.-f.-kennedy';
-- AND fechas_de_inscripcion BETWEEN '01-02-2021' AND '01-09-2020';
-- Falta corregir las fechas de la segunda universidad y agregar columna location