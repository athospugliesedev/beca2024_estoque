Qual o top 5 dos produtos que mais demoram pra sair?

SELECT prod_id,
MIN(data_hora) AS data_transacao_mais_antiga
FROM silver
WHERE tipo_de_transacao = 0
GROUP BY prod_id
ORDER BY data_transacao_mais_antiga
ASC LIMIT 5;