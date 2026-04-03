with products as (
    select
        product_id,
        category,
        brand,
        price,
        discount_percentage,
        rating,
        stock
    from {{ source('silver', 'dim_products') }}
),

cart_items as (
    select
        product_id,
        quantity,
        line_total,
        discounted_total
    from {{ source('silver', 'fct_cart_items') }}
),

reviews as (
    select
        product_id,
        rating as review_rating
    from {{ source('silver', 'product_reviews') }}
),

product_sales as (
    select
        p.category,
        p.brand,
        p.product_id,
        p.price,
        p.discount_percentage,
        p.rating                                as catalog_rating,
        p.stock,
        coalesce(sum(ci.quantity), 0)           as total_qty_sold,
        coalesce(sum(ci.line_total), 0)         as gross_revenue,
        coalesce(sum(ci.discounted_total), 0)   as net_revenue
    from products p
    left join cart_items ci using (product_id)
    group by
        p.category, p.brand, p.product_id,
        p.price, p.discount_percentage,
        p.rating, p.stock
),

review_summary as (
    select
        p.category,
        round(avg(r.review_rating), 2)          as avg_review_rating,
        count(r.review_rating)                  as total_reviews
    from products p
    left join reviews r using (product_id)
    group by p.category
),

aggregated as (
    select
        ps.category,
        count(distinct ps.product_id)           as product_count,
        count(distinct ps.brand)                as brand_count,
        round(avg(ps.price), 2)                 as avg_price,
        round(avg(ps.discount_percentage), 2)   as avg_discount_pct,
        round(avg(ps.catalog_rating), 2)        as avg_catalog_rating,
        sum(ps.stock)                           as total_stock,
        sum(ps.total_qty_sold)                  as total_qty_sold,
        round(sum(ps.gross_revenue), 2)         as gross_revenue,
        round(sum(ps.net_revenue), 2)           as net_revenue
    from product_sales ps
    group by ps.category
),

final as (
    select
        a.*,
        r.avg_review_rating,
        r.total_reviews,
        round(
            (a.net_revenue / nullif(sum(a.net_revenue) over(), 0)) * 100
        , 2)                                    as revenue_share_pct
    from aggregated a
    left join review_summary r using (category)
)

select *
from final
order by net_revenue desc