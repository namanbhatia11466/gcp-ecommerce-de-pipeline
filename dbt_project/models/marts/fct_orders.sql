with stg_orders as (

    select * from {{ ref('stg_orders') }}

)

select
    order_id,
    customer_id,
    product_id,
    product_category,
    quantity,
    unit_price,
    amount,
    revenue,
    freight_value,
    currency,
    status,
    value_tier,
    order_size,
    payment_type,
    customer_state,
    order_date,
    created_at
from stg_orders
