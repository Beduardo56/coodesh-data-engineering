-- consulta simples que vê tudo que está dentro da "pasta" do s3 chamado vendas_totais e faz uma agregação usando a coluna mês e ano.
SELECT
concat(mes,'-',ano) AS "Mês do ano",
SUM(quantidade) AS "Quantidade de vendas",
SUM(valor_total) AS "Renda bruta"
FROM "vendas_totais"
GROUP BY
mes, ano;
