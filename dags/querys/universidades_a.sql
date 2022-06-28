-- Universidad flores (No cuenta con la columna localización)
SELECT universidad as university,
       carrera as career,
       fecha_de_inscripcion as inscription_date,
       name,
       fecha_nacimiento as age,
       codigo_postal as postal_code,
       correo_electronico as email
from flores_comahue
WHERE TO_DATE(fecha_de_inscripcion,'yyyy-mm-dd') BETWEEN '2020-09-01' AND '2021-02-01'

UNION

-- Universidad vialla maria (no cuenta con la columna codigo posta)
SELECT universidad as university,
       carrera as career,
       fecha_de_inscripcion as inscription_date,
       nombre as name,
       fecha_nacimiento as age,
       localidad as location,
       email as email
from salvador_villa_maria
WHERE TO_DATE(fecha_de_inscripcion,'dd-mon-yy') BETWEEN '2020-09-01' AND '2021-02-01'

ORDER BY university;