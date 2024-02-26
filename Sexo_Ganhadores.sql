SELECT
    C.Sexo,
    COUNT(CASE WHEN R.Winning > 0 THEN 1 END) AS Total_Ganhadores,
    COUNT(*) AS Total_Jogadores,
    CAST(COUNT(CASE WHEN R.Winning > 0 THEN 1 END) AS FLOAT) / CAST(COUNT(*) AS FLOAT) AS Proporção_Ganhadores
FROM resultado R
JOIN clientes C ON R.Clientes_id = C.Id
GROUP BY C.Sexo
ORDER BY Proporção_Ganhadores DESC