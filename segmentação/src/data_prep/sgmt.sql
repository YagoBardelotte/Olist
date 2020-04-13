SELECT T1.*,
    CASE WHEN PCT_RECEITA <= 0.5 AND PCT_FREQ <= 0.5 THEN 'BX VLR BX FREQ'
    WHEN PCT_RECEITA > 0.5 AND PCT_FREQ <= 0.5 THEN 'ALTO VALOR'
    WHEN PCT_RECEITA <= 0.5 AND PCT_FREQ > 0.5 THEN 'ALTA FREC'
    WHEN PCT_RECEITA < 0.9 OR PCT_FREQ < 0.9 THEN 'PRODUTIVO'
    ELSE 'SUPER PRODUTIVO'
    END AS SEGMENTO_VALOR_FREQ,

    CASE WHEN QTD_DIAS_BASE <= 60 THEN 'INICIO'
    WHEN QTDE_DIAS_ULT_VENDA >= 300 THEN 'RETENÇÃO'
    ELSE 'ATIVO'
    END AS SEGMENTO_VIDA,

    '{date_end}' AS DT_SGMT 

FROM (

    SELECT T1.*,
        PERCENT_RANK () OVER (ORDER BY RECEITA_TOTAL) AS PCT_RECEITA,
        PERCENT_RANK () OVER (ORDER BY QTDE_PEDIDOS) AS PCT_FREQ

    FROM(

        SELECT T2.seller_id,
                SUM(T2.price) AS RECEITA_TOTAL,
                COUNT(DISTINCT T1.order_id) AS QTDE_PEDIDOS,
                COUNT(T2.product_id) AS QTDE_PRODUTOS,
                COUNT(DISTINCT T2.product_id) AS QTDE_PRODUTOS_DIST,
                MIN( CAST( JULIANDAY('{date_end}') - JULIANDAY(T1.order_approved_at ) AS INT)) AS QTDE_DIAS_ULT_VENDA,
                MAX( CAST(JULIANDAY('{date_end}') - JULIANDAY(DT_INICIO) AS INT)) AS QTD_DIAS_BASE

        FROM tb_orders AS T1

        LEFT JOIN tb_order_items AS T2
        ON T1.order_id = T2.order_id

        LEFT JOIN (

            SELECT T2.seller_id, 
                    MIN(DATE(T1.order_approved_at)) AS DT_INICIO

            FROM tb_orders AS T1

            LEFT JOIN tb_order_items AS T2
            ON T1.order_id = T2.order_id

            GROUP BY T2.seller_id
        ) AS T3
        ON T2.seller_id = T3.seller_id

        WHERE T1.order_approved_at BETWEEN '{date_init}' AND '{date_end}'

        GROUP BY T2.seller_id

    ) AS T1

) AS T1

WHERE seller_id IS NOT NULL