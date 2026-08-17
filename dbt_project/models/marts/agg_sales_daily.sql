with stg_orders as (

    select * from {{ ref('stg_orders') }}

)

select
    order_date,
    status,
    customer_state,
    count(distinct order_id) as order_count,
    count(*) as line_count,
    sum(revenue) as total_revenue,
    round(avg(revenue), 2) as avg_line_value
from stg_orders
group by order_date, status, customer_state
