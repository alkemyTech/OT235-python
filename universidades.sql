SELECT  university, 
        career, 
        inscription_date,
        first_name,
        last_name,
        gender,
        age,
        postal_code,
        location,
        email
FROM Universidad Del Cine 
where inscription_date between cast('01/9/2020' as date) and cast('01/02/2021' as date);

#

SELECT  university, 
        career, 
        inscription_date,
        first_name,
        last_name,
        gender,
        age,
        postal_code,
        location,
        email
FROM UBA 
where inscription_date between cast('01/9/2020' as date) and cast('01/02/2021' as date);

