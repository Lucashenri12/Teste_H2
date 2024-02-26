SELECT
    strftime('%Y-%m', Data_acesso) AS Mês,
    SUM(Rake) AS Rake_Mensal
FROM resultado
GROUP BY Mês;