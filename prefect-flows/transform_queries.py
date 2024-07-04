create_t_cast_types = """
create table if not exists product_sales.t_cast_types (
    order_id int,
    product varchar(255),
    qty_ordered int,
    price_each numeric(10,2),
    order_date timestamp,
    purchase_address varchar(255),
    store varchar(255),
    payment_method varchar(255)   
)
"""

t_cast_types = """
truncate table product_sales.t_cast_types;
insert into product_sales.t_cast_types
with t_rm_nulls_dups as (
    select distinct *
    from product_sales.raw_sales_landing
    where "Order ID" != 'Order ID' and "Order ID" is not null
),
final_t_cast_types as (
select
    "Order ID"::int as order_id,
    "Product"::varchar(255) as product,
    "Quantity Ordered"::int as qty_ordered,
    "Price Each"::numeric(10,2) as price_each,
    case
        when
            length("Order Date") = 14 then strptime("Order Date", '%m/%d/%y %H:%M')
        else strptime("Order Date", '%m/%d/%Y %H:%M')
    end as order_time_stamp,
    "Purchase Address"::varchar(255) as purchase_address,
    "Store"::varchar(255) as store,
    "Payment Method"::varchar(255) as payment_method
from t_rm_nulls_dups
)
select *
from final_t_cast_types
"""


create_final_product_sales = """
create table if not exists product_sales.final_product_sales (
    order_sk_id int,
    order_id int,
    product varchar(255),
    qty_ordered int,
    price_each numeric(10,2),
    order_time_stamp timestamp,
    purchase_address varchar(255),
    store varchar(255),
    payment_method varchar(255)
)
"""

final_product_sales = """
truncate table product_sales.final_product_sales;
insert into product_sales.final_product_sales
with pre_final_product_sales as (
	select
		row_number() over(order by order_time_stamp asc) as order_sk_id,
		order_id,
		product,
		qty_ordered,
		price_each,
		order_time_stamp,
		purchase_address,
		store,
		payment_method
	from product_sales.t_cast_types
	where extract(year from order_time_stamp) = 2019
	order by order_time_stamp asc
)
select *
from pre_final_product_sales

"""


create_dim_order_dates = """
create table if not exists product_sales.dim_order_dates(
    orderdate_sk_id int,
    order_time_stamp timestamp,
    order_date_part DATE,
    year int,
    month int,
    month_name varchar,
    day_name varchar,
    day_month int,
    day_name_num int,
    hour int,
    minute int
)
"""

dim_order_dates = """
insert into product_sales.dim_order_dates
with pre_dim_order_dates as (
	select
		row_number() over(order by order_time_stamp asc) as orderdate_sk_id,
		order_time_stamp,
        cast(order_time_stamp as DATE) as order_date_part,
		extract(year from order_time_stamp) as year,
		extract(month from order_time_stamp) as month,
		monthname(order_time_stamp) as month_name,
		dayname(order_time_stamp) as day_name,
		extract(day from order_time_stamp) as day_month,
		extract(dow from order_time_stamp) as day_name_num,
		extract(hour from order_time_stamp) as hour,
		extract(minute from order_time_stamp) as minute
	from (select
		distinct order_time_stamp
	from product_sales.final_product_sales) distinct_order_date
)

select *
from pre_dim_order_dates
order by order_time_stamp asc
"""


create_dim_stores = """
create table if not exists product_sales.dim_stores (
store_sk_id int,
store_id int,
store varchar
)
"""

dim_stores = """
truncate table product_sales.dim_stores;
insert into product_sales.dim_stores
with pre_dim_stores as (
select
    row_number() over(order by lower(store) asc) as store_sk_id,
    row_number() over(order by lower(store) asc) as store_id,
    store
from (
    select distinct store
    from product_sales.final_product_sales ) as distinct_store 
)

select distinct *
from pre_dim_stores
order by store_id asc
"""


create_dim_payment_methods = """
create table if not exists product_sales.dim_payment_methods (
    pay_method_sk_id int,
    pay_method_id int,
    payment_method varchar 
)
"""

dim_payment_methods = """
truncate table product_sales.dim_payment_methods;
insert into product_sales.dim_payment_methods
with pre_dim_payment_methods as (
select
    row_number() over(order by payment_method asc) as pay_method_sk_id,
    row_number() over(order by payment_method asc) as pay_method_id,
    payment_method
from (
select distinct payment_method
from product_sales.final_product_sales
order by payment_method asc ) distinct_pay_methods   
)
select *
from pre_dim_payment_methods
"""

create_dim_products = """
create table if not exists product_sales.dim_products(
    product_sk_id int,
    product_id int,
    product varchar,
    price_each numeric(10,2)
)
"""

dim_products = """
truncate table product_sales.dim_products;
insert into product_sales.dim_products
with pre_dim_products as (
    select
        row_number() over(order by lower(product) asc) as product_sk_id,
        row_number() over(order by lower(product) asc) as product_id,
        product,
        price_each
    from (
        select distinct product, price_each
        from product_sales.final_product_sales
        order by lower(product) asc
    ) distinct_products
)
select distinct *
from pre_dim_products
order by product_id asc
"""

create_dim_customers = """
create table if not exists product_sales.dim_customers(
    customer_sk_id int,
    customer_id int,
    purchase_address varchar,
    city varchar,
    cust_state_abbrv varchar,
    state_name varchar,
    capital varchar,
    latitude varchar,
    longitude varchar
)
"""

dim_customers = """
truncate table product_sales.dim_customers;
insert into product_sales.dim_customers
with stg_dim_customers as (
select
    row_number() over(order by purchase_address asc) customer_sk_id,
    row_number() over(order by purchase_address asc) customer_id,
    purchase_address,
    ltrim(split_part(purchase_address,',',2))::varchar as city,
    split_part(ltrim(split_part(purchase_address,',',3)),' ',1)::varchar as cust_state_abbrv
from ( select distinct purchase_address
from product_sales.final_product_sales ) distinct_address
),
    pre_dim_customers as (
        select
        a.customer_sk_id,
        a.customer_id,
        a.purchase_address,
        a.city,
        a.cust_state_abbrv,
        b.state_name,
        b.capital,
        b.latitude,
        b.longitude
        from stg_dim_customers as a
    left join product_sales.dim_states as b on a.cust_state_abbrv = b.state_abbrv
)

select *
from pre_dim_customers
"""

create_final_fact_sales = """
create table if not exists product_sales.final_fact_sales (
    order_sk_id int,
	order_id int,
	customer_sk_id int,
	product_sk_id int,
	qty_ordered int,
	total_price numeric(10,2),
	purchase_address_id int,
	orderdate_sk_id int,
	store_sk_id int,
	pay_method_sk_id int
)
"""

final_fact_sales ="""
truncate table product_sales.final_fact_sales;
insert into product_sales.final_fact_sales
with pre_final_fact_sales as (
	select
	a.order_sk_id,
	a.order_id,
	c.customer_sk_id,
	b.product_sk_id,
	a.qty_ordered,
	(a.qty_ordered * b.price_each)::numeric(10,2) as total_price,
	c.customer_sk_id as purchase_address_id,
	f.orderdate_sk_id,
	d.store_sk_id,
	e.pay_method_sk_id
	
	from product_sales.final_product_sales as a
	left join product_sales.dim_products as b on a.product = b.product
	left join product_sales.dim_customers as c on a.purchase_address = c.purchase_address
	left join product_sales.dim_stores as d on a.store = d.store
	left join product_sales.dim_payment_methods as e on a.payment_method = e.payment_method
	left join product_sales.dim_order_dates as f on a.order_time_stamp = f.order_time_stamp
	order by order_sk_id asc
)
select *
from pre_final_fact_sales
"""



create_mart_product_sales = """
create table if not exists product_sales.mart_product_sales(
	order_id int,
	customer_id int,
	product varchar,
	price_each numeric(10,2),
	qty_ordered int,
	total_price numeric(10,2),
	order_time_stamp timestamp,
	order_date_part DATE,
    year int,
	month int,
	month_name varchar,
	day_name varchar,
	day_name_num int,
	day_month int,
	purchase_address varchar,
	city varchar,
	state_name varchar,
    capital varchar,
    latitude varchar,
    longitude varchar,
	store varchar,
	payment_method varchar
)
"""

mart_product_sales = """
truncate table product_sales.mart_product_sales;
insert into product_sales.mart_product_sales
with pre_mart_product_sales as (
    select
	a.order_id,
	c.customer_id,
	b.product,
	b.price_each,
	a.qty_ordered,
	a.total_price,
	f.order_time_stamp,
    f.order_date_part,
    f.year,
	f.month,
	f.month_name,
	f.day_name,
	f.day_name_num,
	f.day_month,
	c.purchase_address,
	c.city,
	c.state_name,
    c.capital,
    c.latitude,
    c.longitude,
	d.store,
	e.payment_method
	from product_sales.final_fact_sales as a
	left join product_sales.dim_products as b on a.product_sk_id = b.product_sk_id
	left join product_sales.dim_customers as c on a.customer_sk_id = c.customer_sk_id
	left join product_sales.dim_stores as d on a.store_sk_id = d.store_sk_id
	left join product_sales.dim_payment_methods as e on a.pay_method_sk_id = e.pay_method_sk_id
	left join product_sales.dim_order_dates as f on a.orderdate_sk_id = f.orderdate_sk_id
)
select *
from pre_mart_product_sales
order by order_time_stamp asc
"""