with source as (

    select * from {{ source('raw', 'orders') }}

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
    created_at,
    processed_at,
    loaded_at
from source
