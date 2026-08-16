with stg_orders as (

    select * from {{ ref('stg_orders') }}

)

select
    order_id,
    user_id,
    product_id,
    product_name,
    quantity,
    unit_price,
    amount,
    revenue,
    currency,
    status,
    value_tier,
    order_size,
    country,
    order_date,
    created_at
from stg_orders
