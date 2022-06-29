-- Goup-E:
-- Universidad Nacional De La Pampa
-- Universidad Abierta Interamericana
-- Queries to later process information on: 
--      university
--      career
--      inscription_date
--      first_name
--      last_name
--      gender
--      age
--      postal_code
--      location
--      email

-- query Universidad Nacional De La Pampa
SELECT universidad, 
    carrerra,
    fechaiscripccion,
    nombrre,
    sexo,
    nacimiento,
    codgoposstal,
    eemail
FROM moron_nacional_pampa
-- filter by university and date
WHERE universidad like '%pampa%' 
    AND TO_DATE(fechaiscripccion,'%dd/%mm/%YYYY') BETWEEN '2020-09-01' AND '2021-02-01'; --format: YYYY-MM-DD

-- query Universidad Abierta Interamericana
SELECT univiersities, 
    carrera,
    inscription_dates,
    names,
    sexo,
    fechas_nacimiento,
    localidad,
    email
FROM rio_cuarto_interamericana 
-- filter by university and date
WHERE univiersities like '%interamericana%' 
    AND TO_DATE(inscription_dates, '%dd/%MON/%YY') BETWEEN '2020-09-01' AND '2021-02-01'; --format: YYYY-MM-DD

