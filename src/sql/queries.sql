SELECT DT_SGMT, 
       COUNT (DISTINCT seller_id)
FROM tb_seller_sgmt
GROUP BY DT_SGMT;