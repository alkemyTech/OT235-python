select universidad as university,
       careers as caree,
	   to_date(fecha_de_inscripcion,'DD-Mon-YY') as inscription_date,
	   names as full_name,
	   sexo as gender,
	   to_date(birth_dates,'DD-Mon-YY') as birth_date,
	   codigo_postal as postal_code,
	   correos_electronicos as email	   
from palermo_tres_de_febrero
where universidad like '_universidad_de_palermo'
and to_date(fecha_de_inscripcion,'DD-Mon-YY') between '2020-09-01' and '2021-02-01' 