select product_id, reviewer_name, rating
from {{ ref('product_reviews') }}
where rating < 1 or rating > 5
