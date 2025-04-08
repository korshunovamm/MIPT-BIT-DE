-- CUSTOMER MANAGED
CREATE TABLE lab.customer_managed (
  customer_id INT,
  first_name STRING,
  last_name STRING,
  gender STRING,
  DOB STRING,
  job_title STRING,
  job_industry_category STRING,
  wealth_segment STRING,
  deceased_indicator STRING,
  owns_car STRING,
  address STRING,
  postcode INT,
  state STRING,
  country STRING,
  property_valuation INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- CUSTOMER EXTERNAL
CREATE EXTERNAL TABLE lab.customer_external (
  customer_id INT,
  first_name STRING,
  last_name STRING,
  gender STRING,
  DOB STRING,
  job_title STRING,
  job_industry_category STRING,
  wealth_segment STRING,
  deceased_indicator STRING,
  owns_car STRING,
  address STRING,
  postcode INT,
  state STRING,
  country STRING,
  property_valuation INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/input'
TBLPROPERTIES ("skip.header.line.count"="1");

-- TRANSACTION MANAGED
CREATE TABLE lab.transaction_managed (
  transaction_id INT,
  product_id INT,
  customer_id INT,
  transaction_date STRING,
  online_order STRING,
  order_status STRING,
  brand STRING,
  product_line STRING,
  product_class STRING,
  product_size STRING,
  list_price FLOAT,
  standard_cost FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- TRANSACTION EXTERNAL
CREATE EXTERNAL TABLE lab.transaction_external (
  transaction_id INT,
  product_id INT,
  customer_id INT,
  transaction_date STRING,
  online_order STRING,
  order_status STRING,
  brand STRING,
  product_line STRING,
  product_class STRING,
  product_size STRING,
  list_price FLOAT,
  standard_cost FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/input'
TBLPROPERTIES ("skip.header.line.count"="1");