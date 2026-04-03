select
    cart_id,
    total,
    discounted_total,
    savings_amount
from {{ ref('fct_carts') }}
where discounted_total > total