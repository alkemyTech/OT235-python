WITH refining_data as(
select university,
	   career,
	   to_date(inscription_date,'YYYY-MM-DD') as inscription_date,
	   nombre as names,
	   sexo as gender,
	   to_date(birth_date,'YYYY-MM-DD') as birth_date,
	   location,
	   email
from jujuy_utn
where university like 'universidad nacional de jujuy')
select * 
from refining_data
where inscription_date between '2020-09-01' and '2021-02-01'
	   
	   