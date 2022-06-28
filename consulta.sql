-- datos sin normaliazar

SELECT universities,careers,inscription_dates,"names",sexo,birth_dates,locations,emails FROM lat_sociales_cine
WHERE inscription_dates BETWEEN CAST('01/9/2020' AS DATE) AND CAST('01/02/2021' AS DATE);


-- select universidades,carreras,fechas_de_inscripcion,nombres,sexo,fechas_nacimiento,codigos_postales,emails from uba_kenedy;
