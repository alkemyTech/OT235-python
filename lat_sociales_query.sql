SELECT  universities,
        careers,
        inscription_dates AS inscription_date,
		SUBSTRING(nombre, 1, POSITION('_' IN names)) AS first_name,
        SUBSTRING(nombre FROM POSITION('_' IN names)) AS last_name,
        sexo AS gender,
		EXTRACT(YEAR FROM age(TIMESTAMP 'NOW()',TO_DATE(birth_dates,'DD-MM-YYYY'))) AS age,
		locations AS "location",
--      codigos_postales as postal_code,
        emails AS email
FROM lat_sociales_cine 
WHERE universities LIKE '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES'
AND inscription_dates BETWEEN '01-02-2021' AND '01-09-2020';