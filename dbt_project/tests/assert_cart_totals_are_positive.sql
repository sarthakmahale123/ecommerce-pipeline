select
    cart_id,
    user_id,
    total,
    discounted_total
from {{ ref('fct_carts') }}
where total <= 0
   or discounted_total <= 0