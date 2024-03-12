Qual é o saldo de estoque atual por produto?

SELECT prod_id, nome_produto, SUM(saldo) AS saldo_por_produto
FROM estoque.produto_estoque_gold
GROUP BY prod_id, nome_produto
LIMIT 10;
