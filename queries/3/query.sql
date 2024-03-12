Qual o top 5 dos produtos que mais demoram pra sair?

SELECT prod_id, nome_produto, MIN(data_hora) AS data_transacao_mais_antiga 
FROM estoque.produto_estoque_gold WHERE tipo_de_transacao = 0 GROUP BY prod_id, nome_produto ORDER BY data_transacao_mais_antiga LIMIT 5;
