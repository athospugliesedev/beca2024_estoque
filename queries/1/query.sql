Qual é o saldo de estoque atual por produto?

SELECT prod_id,
SUM(saldo) AS saldo_por_produto
FROM silver
GROUP BY prod_id;