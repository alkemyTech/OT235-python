# Realizamos la consulta especifica de los alumnos inscriptos en las universidadesdes espeficicadas y las fechas 

select university,career,inscription_date,first_name,last_name,gender,DATEDIFF(year, date_bith , GETDATE())  as age,
postal_code,location,email
from Information
where university in ('Universidad Tecnológica Nacional' ,'Universidad Nacional De Tres De Febrero') 
and inscription_date between  '01/9/2020' and '01/02/2021'



