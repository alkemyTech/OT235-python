SELECT  universities,
        careers as career,
        inscription_dates AS inscription_date,
		SUBSTRING(names from 1 for (POSITION('-' in names) - 1)) AS first_name,
		SUBSTRING(names from (POSITION('-' in names) + 1) for LENGTH(names)) AS last_name,
        sexo AS gender,
		EXTRACT(YEAR FROM age(TIMESTAMP 'NOW()',TO_DATE(birth_dates,'DD-MM-YYYY'))) AS age,
		locations AS "location",
        emails AS email
FROM lat_sociales_cine 
WHERE universities LIKE '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES'
AND inscription_dates BETWEEN '01-02-2021' AND '01-09-2020';