with source as (
    select * from {{ source('bronze', 'carts_raw') }}
),

cleaned as (
    select
        id                                                          as cart_id,
        user_id,
        total,
        discounted_total,
        total_products,
        total_quantity,
        round(total - discounted_total, 2)                          as savings_amount,
        round(
            (total - discounted_total) / nullif(total, 0) * 100
        , 2)                                                        as savings_pct,
        _ingested_at
    from source
    where id is not null
),

deduped as (
    select *,
        row_number() over (
            partition by cart_id
            order by _ingested_at desc
        ) as rn
    from cleaned
)

select * exclude (rn)
from deduped
where rn = 1