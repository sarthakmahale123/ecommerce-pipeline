select cart_id, product_id, price, quantity, line_total
from {{ ref('fct_cart_items') }}
where abs(line_total - (price * quantity)) > (price * quantity * 0.01)
