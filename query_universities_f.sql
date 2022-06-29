-- SQL queries to get students data from two universities:
--   - Universidad De Morón
--   - Universidad Nacional De Río Cuarto

-- Universidad De Morón
select
universidad,
carrerra,
fechaiscripccion,
nombrre,
sexo,
nacimiento,
codgoposstal,
eemail
from moron_nacional_pampa
WHERE 
universidad LIKE '%morón%'
AND TO_DATE(fechaiscripccion,'%dd/%mm/%YYYY') BETWEEN '2020-09-01' AND '2021-02-01'


-- Universidad Nacional De Río Cuarto
select
univiersities,
carrera,
inscription_dates,
names,
sexo,
fechas_nacimiento,
localidad,
email
from rio_cuarto_interamericana
WHERE 
univiersities LIKE '%cuarto%'
AND TO_DATE(inscription_dates,'%dd/%MON/%YY') BETWEEN '01/9/2020' AND '01/02/2021'

