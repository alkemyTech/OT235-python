-- Universidad flores (No cuenta con la columna localización)
SELECT universidad as university,
       carrera as career,
       fecha_de_inscripcion as inscription_date,
       name,
       fecha_nacimiento as age,
       codigo_postal as postal_code,
       correo_electronico as email
FROM flores_comahue
WHERE TO_DATE(fecha_de_inscripcion,'yyyy-mm-dd') BETWEEN '2020-09-01' AND '2021-02-01';