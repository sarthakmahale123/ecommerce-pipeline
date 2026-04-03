select
    category,
    net_revenue
from {{ ref('category_performance') }}
where net_revenue < 0