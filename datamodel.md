dim_states
----
state_id int pk
state_name varchar
state_abbrv varchar
capital varchar
latitude varchar
longitude varchar


dim_customers
---
customer_sk_id int FK
customer_id int
purchase_address varchar
city varchar
state_name varchar
cust_state_abbrv varchar FK >- dim_states.state_abbrv
capital varchar
latitude varchar
longitude varchar


dim_products
---
product_sk_id int
product_id int
product varchar
price numeric


dim_stores
---
store_sk_id int
store_id int
store varchar


dim_payment_methods
---
pay_method_sk_id int
payment_method_id int
payment_method varchar


dim_order_dates
---
orderdate_sk_id serial
order_time_stamp timestamp
order_date_part date
year numeric
month numeric
month_name varchar
day_name varchar
day_month int
day_name_num int
hour numeric
minute numeric


final_fact_sales
----
order_sk_id int PK
order_id int
customer_sk_id int FK >- dim_customers.customer_sk_id
product_sk_id int FK >- dim_products.product_sk_id
qty_ordered int
total_price numeric
orderdate_sk_id int FK >- dim_order_dates.orderdate_sk_id
store_sk_id int FK >- dim_stores.store_sk_id
pay_method_sk_id int FK >- dim_payment_methods.pay_method_sk_id

