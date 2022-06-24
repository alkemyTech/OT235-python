SELECT university,
       caree,
	   TO_DATE(inscription_date,'DD-MM-AAAA')
	   fist_name,
	   last_name,
	   gender,
	   EXTRACT(year from age(timestamp 'now()',TO_DATE(birth_date,'YYYY-MM-DD'))) as age,
	   postal_code,
	   location,
	   email
FROM jujuypalermo
WHERE university LIKE 'Universidad de Palermo'
AND inscripcion_date BETWEEN '01-09-2000' AND '01-02-2001'   
	   
	   
	   
	   
	   
	   