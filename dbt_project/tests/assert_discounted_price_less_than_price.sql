select
    product_id,
    title,
    price,
    discounted_price
from {{ ref('dim_products') }}
where discounted_price > price