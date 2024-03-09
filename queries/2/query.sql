Qual o produto que tem mais saída?

SELECT prod_id,
SUM(qtd) AS total_saida
FROM silver
WHERE tipo_de_transacao = 0
GROUP BY prod_id
ORDER BY total_saida
DESC LIMIT 1;