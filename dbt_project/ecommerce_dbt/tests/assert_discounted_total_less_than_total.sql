select cart_id, total, discounted_total
from {{ ref('fct_carts') }}
where discounted_total > total
