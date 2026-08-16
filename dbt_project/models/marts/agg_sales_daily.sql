with stg_orders as (

    select * from {{ ref('stg_orders') }}

)

select
    order_date,
    status,
    country,
    count(distinct order_id) as order_count,
    sum(revenue) as total_revenue,
    round(avg(revenue), 2) as avg_order_value
from stg_orders
group by order_date, status, country
