
create_raw_sales = """
create table if not exists product_sales.raw_sales_landing as
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

# To be updated. Decimal type for latitude and longitude rounds off decimal values.
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