-- Кол-во подтвержденных транзакций по каждому клиенту
SELECT customer_id, COUNT(*) as confirmed_tx
FROM lab.transaction_partitioned
WHERE order_status = 'Approved'
GROUP BY customer_id;

-- Распределение транзакций по месяцам и сферам деятельности
SELECT
  substr(transaction_date, 1, 7) as month,
  c.job_industry_category,
  COUNT(*) as tx_count
FROM lab.transaction_partitioned t
JOIN lab.customer_parquet c ON t.customer_id = c.customer_id
GROUP BY substr(transaction_date, 1, 7), c.job_industry_category;

-- Клиенты без транзакций
SELECT c.first_name, c.last_name
FROM lab.customer_parquet c
LEFT JOIN lab.transaction_parquet t ON c.customer_id = t.customer_id
WHERE t.customer_id IS NULL;

-- Клиенты с мин/макс суммой транзакций
SELECT first_name, last_name, total
FROM (
  SELECT
    c.first_name,
    c.last_name,
    SUM(t.list_price) as total
  FROM lab.transaction_parquet t
  JOIN lab.customer_parquet c ON c.customer_id = t.customer_id
  WHERE t.list_price IS NOT NULL
  GROUP BY c.first_name, c.last_name
) sub
WHERE total = (
    SELECT MAX(s) FROM (
        SELECT SUM(list_price) as s FROM lab.transaction_parquet GROUP BY customer_id
    ) max_sub
)
OR total = (
    SELECT MIN(s) FROM (
        SELECT SUM(list_price) as s FROM lab.transaction_parquet GROUP BY customer_id
    ) min_sub
);