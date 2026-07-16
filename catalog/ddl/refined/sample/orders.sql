CREATE TABLE IF NOT EXISTS iceberg.refined.sample.orders (
  order_id BIGINT,
  customer_id BIGINT,
  status STRING,
  amount DOUBLE,
  order_date STRING
)
USING iceberg
LOCATION '${warehouse}/refined/sample/orders'
TBLPROPERTIES ('format-version' = '2')
