CREATE TABLE lab.customer_parquet
STORED AS PARQUET
AS SELECT * FROM lab.customer_managed;

CREATE TABLE lab.transaction_parquet
STORED AS PARQUET
AS SELECT * FROM lab.transaction_managed;