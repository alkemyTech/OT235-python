--Universidad flores
SELECT university,
       career,
       inscription_date,
       first_name,
       last_name,
       gender,
       age,
       postal_code,
       location,
       email
FROM Universidad_de_Flores
WHERE inscription_date BETWEEN CAST ('01/9/2020' as date) and CAST ('01/02/2021' as date);

--Universidad Villa Maria
SELECT university,
       career,
       inscription_date,
       first_name,
       last_name,
       gender,
       age,
       postal_code,
       location,
       email
FROM Universidad_Nacional_De_Villa_María
WHERE inscription_date BETWEEN CAST ('01/9/2020' as date) and CAST ('01/02/2021' as date);
