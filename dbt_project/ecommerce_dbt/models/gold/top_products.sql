with cart_items as (
    select
        product_id,
        product_title,
        price,
        quantity,
        line_total,
        discounted_total,
        discount_pct
    from {{ source('silver', 'fct_cart_items') }}
),

product_info as (
    select
        product_id,
        category,
        brand,
        rating,
        stock,
        availability_status
    from {{ source('silver', 'dim_products') }}
),

reviews_summary as (
    select
        product_id,
        round(avg(rating), 2)                   as avg_review_rating,
        count(*)                                as review_count
    from {{ source('silver', 'product_reviews') }}
    group by product_id
),

joined as (
    select
        ci.product_id,
        ci.product_title,
        p.category,
        p.brand,
        p.rating                                as catalog_rating,
        r.avg_review_rating,
        r.review_count,
        p.stock,
        p.availability_status,
        count(*)                                as times_ordered,
        sum(ci.quantity)                        as total_qty_sold,
        round(sum(ci.line_total), 2)            as gross_revenue,
        round(sum(ci.discounted_total), 2)      as net_revenue,
        round(avg(ci.discount_pct), 2)          as avg_discount_pct
    from cart_items ci
    left join product_info p    using (product_id)
    left join reviews_summary r using (product_id)
    group by
        ci.product_id, ci.product_title,
        p.category, p.brand, p.rating,
        r.avg_review_rating, r.review_count,
        p.stock, p.availability_status
)

select *
from joined
order by net_revenue desc