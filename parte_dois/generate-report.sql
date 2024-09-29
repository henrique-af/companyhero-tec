CREATE OR REPLACE TABLE `{project_id}.silver.assinaturas_ativas` AS
SELECT
    s.id_assinatura,
    s.plano,
    s.unidade,
    s.cupom,
    s.data_criacao_assinatura,
    s.data_vencimento_assinatura,
    s.assinatura_ativa,
    SUM(CASE
        WHEN i.total_paid LIKE 'R$%' THEN REPLACE(REPLACE(i.total_paid, 'R$', ''), '.', '')
        WHEN i.total_paid LIKE 'BRL%' THEN REPLACE(REPLACE(i.total_paid, 'BRL', ''), '.', '')
        ELSE 0
    END) AS total_receita_paga
FROM
    `{project_id}.silver.Subscriptions` AS s
JOIN
    `{project_id}.silver.Invoices` AS i
ON
    s.id_assinatura = i.id_assinatura
WHERE
    s.assinatura_ativa = 'TRUE'
GROUP BY
    s.id_assinatura, s.plano, s.unidade, s.cupom, s.data_criacao_assinatura, s.data_vencimento_assinatura, s.assinatura_ativa;