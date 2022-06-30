select university,
	   career,
	   to_date(inscription_date,'YYYY-MM-DD') as inscription_date,
	   nombre as full_name,
	   sexo as gender,
	   to_date(birth_date,'YYYY-MM-DD') as birth_date,
	   location as locations,
	   email
from jujuy_utn
where university like 'universidad nacional de jujuy'
and to_date(inscription_date,'YYYY-MM-DD') between '2020-09-01' and '2021-02-01'
	   
	   