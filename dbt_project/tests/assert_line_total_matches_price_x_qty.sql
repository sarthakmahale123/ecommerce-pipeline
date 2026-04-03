select
    cart_id,
    product_id,
    product_title,
    price,
    quantity,
    line_total,
    round(price * quantity, 2) as expected_total
from {{ ref('fct_cart_items') }}
where abs(line_total - (price * quantity)) > (price * quantity * 0.01)