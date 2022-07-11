SELECT  universities, 
        careers, 
        inscription_dates,
        names,
        sexo,
        birth_dates,
        locations,
        emails
FROM public.lat_sociales_cine
where universities='UNIVERSIDAD-DEL-CINE' and 
to_date(inscription_dates,'DD-MM-YYYY') between cast('01/9/2020' as date) and cast('01/02/2021' as date);

#

SELECT  universidades, 
        carreras, 
        fechas_de_inscripcion,
        nombres,
        sexo,
        fechas_nacimiento,
        codigos_postales,
        emails
FROM public.uba_kenedy
where universidades='universidad-de-buenos-aires' 
and to_date(fechas_de_inscripcion,'YY-Mon-DD')  between cast('01/9/2020' as date) and cast('01/02/2021' as date);

