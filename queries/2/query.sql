Qual o produto que tem mais saída?

SELECT prod_id, nomeproduto, SUM(qtd) AS total_saidas
FROM estoque.produto_estoque_gold
WHERE tipo_de_transacao = 0
GROUP BY prod_id, nomeproduto
ORDER BY total_saidas DESC
LIMIT 5;
