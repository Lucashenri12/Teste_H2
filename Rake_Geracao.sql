SELECT
    CASE
        WHEN strftime('%Y', C.Data_nascimento) BETWEEN '1925' AND '1940' THEN 'Veteranos'
        WHEN strftime('%Y', C.Data_nascimento) BETWEEN '1941' AND '1959' THEN 'Baby Boomers'
        WHEN strftime('%Y', C.Data_nascimento) BETWEEN '1960' AND '1979' THEN 'Geração X'
        WHEN strftime('%Y', C.Data_nascimento) BETWEEN '1980' AND '1995' THEN 'Geração Y'
        WHEN strftime('%Y', C.Data_nascimento) BETWEEN '1996' AND '2010' THEN 'Geração Z'
        ELSE 'Geração Alpha'
    END AS Geração,
    SUM(R.Rake) AS Rake_Total
FROM resultado R
JOIN clientes C ON R.Clientes_id = C.Id
GROUP BY Geração;