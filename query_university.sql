-- SQL queries to get students data from two universities:
--   -Universidad De Morón
--   -Universidad Nacional De Río Cuarto

SELECT
university,
career,
inscription_date,
first_name,
last_name,
gender,
YEAR(CURRENT_TIMESTAMP) - YEAR(date_of_birth) as age,
postal_code,
location,
email
FROM universidades
WHERE 
university LIKE 'Morón'
AND inscription_date between '01/9/2020' AND '01/02/2021'



SELECT
university,
career,
inscription_date,
first_name,
last_name,
gender,
YEAR(CURRENT_TIMESTAMP) - YEAR(date_of_birth) as age,
postal_code,
location,
email
FROM universidades
WHERE university LIKE 'Río Cuarto'
AND inscription_date between '01/9/2020' AND '01/02/2021'

