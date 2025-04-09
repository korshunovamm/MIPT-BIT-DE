-- 1. Создание временных таблиц для исходных данных
CREATE EXTERNAL TABLE customer_csv_temp (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    dob STRING,
    job_title STRING,
    job_industry_category STRING,
    wealth_segment STRING,
    deceased_indicator STRING,
    owns_car STRING,
    address STRING,
    postcode STRING,
    state STRING,
    country STRING,
    property_valuation STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ";",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION '/input/customer'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE transaction_csv_temp (
    transaction_id STRING,
    product_id STRING,
    customer_id STRING,
    transaction_date STRING,
    online_order STRING,
    order_status STRING,
    brand STRING,
    product_line STRING,
    product_class STRING,
    product_size STRING,
    list_price STRING,
    standard_cost STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ";",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION '/input/transaction'
TBLPROPERTIES ("skip.header.line.count"="1");





-- 2. Создание и заполнение managed таблиц
CREATE TABLE managed_customer (
    customer_id INT,
    first_name STRING,
    last_name STRING,
    gender STRING,
    dob DATE,
    job_title STRING,
    job_industry_category STRING,
    wealth_segment STRING,
    deceased_indicator STRING,
    owns_car STRING,
    address STRING,
    postcode STRING,
    state STRING,
    country STRING,
    property_valuation INT
)
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE managed_customer
SELECT 
    CAST(customer_id AS INT),
    first_name,
    last_name,
    gender,
    to_date(dob),
    job_title,
    job_industry_category,
    wealth_segment,
    deceased_indicator,
    owns_car,
    address,
    postcode,
    state,
    country,
    CAST(property_valuation AS INT)
FROM customer_csv_temp;

CREATE TABLE managed_transaction (
    transaction_id INT,
    product_id INT,
    customer_id INT,
    transaction_date DATE,
    online_order STRING,
    order_status STRING,
    brand STRING,
    product_line STRING,
    product_class STRING,
    product_size STRING,
    list_price FLOAT,
    standard_cost FLOAT
)
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE managed_transaction
SELECT 
    CAST(transaction_id AS INT),
    CAST(product_id AS INT),
    CAST(customer_id AS INT),
    to_date(from_unixtime(unix_timestamp(transaction_date, 'dd.MM.yyyy'))),
    online_order,
    order_status,
    brand,
    product_line,
    product_class,
    product_size,
    CAST(regexp_replace(list_price, ',', '.') AS FLOAT),
    CAST(regexp_replace(standard_cost, ',', '.') AS FLOAT)
FROM transaction_csv_temp;

-- 3. Создание и заполнение external таблиц
CREATE EXTERNAL TABLE external_customer (
    customer_id INT,
    first_name STRING,
    last_name STRING,
    gender STRING,
    dob DATE,
    job_title STRING,
    job_industry_category STRING,
    wealth_segment STRING,
    deceased_indicator STRING,
    owns_car STRING,
    address STRING,
    postcode STRING,
    state STRING,
    country STRING,
    property_valuation INT
)
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/customer_external';

INSERT OVERWRITE TABLE external_customer
SELECT * FROM managed_customer;

CREATE EXTERNAL TABLE external_transaction (
    transaction_id INT,
    product_id INT,
    customer_id INT,
    transaction_date DATE,
    online_order STRING,
    order_status STRING,
    brand STRING,
    product_line STRING,
    product_class STRING,
    product_size STRING,
    list_price FLOAT,
    standard_cost FLOAT
)
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/transaction_external';

INSERT OVERWRITE TABLE external_transaction
SELECT * FROM managed_transaction;

show tables;



CREATE TABLE parquet_customer STORED AS PARQUET AS
SELECT * FROM managed_customer;

CREATE TABLE parquet_transaction STORED AS PARQUET AS
SELECT * FROM managed_transaction;

show tables;





DROP TABLE IF EXISTS partitioned_transaction;

CREATE TABLE partitioned_transaction (
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
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'serialization.format'='1'
);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=10000;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.optimize.sort.dynamic.partition.threshold=0;
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;


INSERT OVERWRITE TABLE partitioned_transaction PARTITION(transaction_date)
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
    date_format(transaction_date, 'yyyy-MM-dd') AS transaction_date  
FROM managed_transaction
DISTRIBUTE BY date_format(transaction_date, 'yyyy-MM-dd');

SELECT * FROM partitioned_transaction LIMIT 5;