SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE lab.transaction_partitioned (
  transaction_id INT,
  product_id INT,
  customer_id INT,
  online_order STRING,
  order_status STRING,
  brand STRING,
  product_line STRING,
  product_class STRING,
  product_size STRING,
  list_price FLOAT,
  standard_cost FLOAT
)
PARTITIONED BY (transaction_date STRING)
STORED AS PARQUET;

INSERT INTO TABLE lab.transaction_partitioned
PARTITION (transaction_date)
SELECT
  transaction_id,
  product_id,
  customer_id,
  online_order,
  order_status,
  brand,
  product_line,
  product_class,
  product_size,
  list_price,
  standard_cost,
  regexp_replace(transaction_date, '\\\\s.*$', '') as transaction_date
FROM lab.transaction_managed;