
create_raw_sales = """
create table product_sales.raw_sales_landing as
with pre_raw_sales_landing as(
	select *
	from raw_sales_df
)
select *
from pre_raw_sales_landing
"""

trunc_tables = """
truncate table product_sales.raw_sales_landing;
truncate table product_sales.raw_dim_states;
truncate table product_sales.dim_states;
"""

insert_raw_sales = """
insert into product_sales.raw_sales_landing
select *
from raw_sales_df
"""

insert_raw_dim_states = """
insert into product_sales.raw_dim_states
select *
from states_df
"""

############################################################################


#To be updated, decimal conversion of latitude and longitude rounds off values
trans_dim_states = """
insert into product_sales.dim_states
select
state_id::int as state_id,
state_name::varchar(255) as state_name,
state_abbrv::varchar(255) as state_abbrv,
capital::varchar(255) as capital,
latitude::decimal(13,7) as latitude,
longitude::decimal(13,7) as longitude
from product_sales.raw_dim_states
"""

q_t_rm_nulls_dups = """
create table product_sales.t_rm_nulls_dups as
with pre_remove_nulls_dups as (
  select *
  from product_sales.raw_sales_landing
  where "Order ID" != 'Order ID' and "Order ID" is not null
)
select *
from pre_remove_nulls_dups
"""

q_t_cast_types = """
create table product_sales.t_cast_types as
with pre_t_cast_types as (
select
    "Order ID"::int as order_id,
    "Product"::varchar(255) as product,
    "Quantity Ordered"::int as qty_ordered,
    "Price Each"::numeric(10,2) as price_each,
    case
    	when
            length("Order Date") = 14 then strptime("Order Date", '%m/%d/%y %H:%M')
        else strptime("Order Date", '%m/%d/%Y %H:%M')
    end as order_date,
    "Purchase Address"::varchar(255) as purchase_address,
    "Store"::varchar(255) as store,
    "Payment Method"::varchar(255) as payment_method
from product_sales.t_rm_nulls_dups
)

select *
from pre_t_cast_types
"""

t_cast_types = """
    with t_rm_nulls_dups as (
        select distinct *
        from df
        where "Order ID" != 'Order ID' and "Order ID" is not null
    ),
    t_cast_types as (
    select
        "Order ID"::int as order_id,
        "Product"::varchar(255) as product,
        "Quantity Ordered"::int as qty_ordered,
        "Price Each"::numeric(10,2) as price_each,
        case
            when
                length("Order Date") = 14 then strptime("Order Date", '%m/%d/%y %H:%M')
            else strptime("Order Date", '%m/%d/%Y %H:%M')
        end as order_date,
        "Purchase Address"::varchar(255) as purchase_address,
        "Store"::varchar(255) as store,
        "Payment Method"::varchar(255) as payment_method
    from t_rm_nulls_dups
    )
    select *
    from t_cast_types
"""


q_final_product_sales = """
create table product_sales.final_product_sales as 
with pre_final_product_sales as (
	select
		row_number() over(order by order_date asc) as order_sk_id,
		order_id,
		product,
		price_each,
		qty_ordered,
		order_date,
		purchase_address,
		store,
		payment_method
	from product_sales.t_cast_types
	where extract(year from order_date) = 2019
	order by order_date asc
)

select *
from pre_final_product_sales

"""