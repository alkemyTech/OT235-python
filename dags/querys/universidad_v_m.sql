-- Universidad vialla maria (no cuenta con la columna codigo posta)
SELECT universidad as university,
       carrera as career,
       fecha_de_inscripcion as inscription_date,
       nombre as name,
       sexo as gender,
       fecha_nacimiento as age,
       localidad as location,
       email as email
FROM salvador_villa_maria
WHERE universidad like '%MARÍA%' OR universidad like '%maría%' AND TO_DATE(fecha_de_inscripcion,'dd-mon-yy') BETWEEN '2020-09-01' AND '2021-02-01';
