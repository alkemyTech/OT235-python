SELECT university,
       caree,
	   TO_DATE(inscription_date,'DD-MM-AAAA') as incripcion_date
	   fist_name,
	   last_name,
	   gender,
	   EXTRACT(year from age(timestamp 'now()',TO_DATE(birth_date,'YYYY-MM-DD'))) as age,
	   postal_code,
	   location,
	   email
FROM jujuypalermo
WHERE university LIKE 'Universidad Nacional de Jujuy'
AND inscripcion_date BETWEEN '2000-09-01'AND '2001-02-01';  