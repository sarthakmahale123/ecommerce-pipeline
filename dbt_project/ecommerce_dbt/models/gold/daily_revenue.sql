with cart_data as (
    select
        date_trunc('day', _ingested_at)         as revenue_date,
        cart_id,
        total,
        discounted_total,
        savings_amount,
        total_quantity
    from {{ source('silver', 'fct_carts') }}
),

aggregated as (
    select
        revenue_date,
        count(distinct cart_id)                 as total_orders,
        round(sum(total), 2)                    as gross_revenue,
        round(sum(discounted_total), 2)         as net_revenue,
        round(sum(savings_amount), 2)           as total_discounts_given,
        round(avg(discounted_total), 2)         as avg_order_value,
        sum(total_quantity)                     as total_items_sold,
        round(
            sum(savings_amount) / nullif(sum(total), 0) * 100
        , 2)                                    as discount_rate_pct
    from cart_data
    group by revenue_date
)

select *
from aggregated
order by revenue_date desc