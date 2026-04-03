with source as (
    select
        id      as cart_id,
        user_id,
        products,
        _ingested_at
    from {{ source('bronze', 'carts_raw') }}
    where products is not null
      and products != '[]'
),

exploded as (
    select
        source.cart_id,
        source.user_id,
        cast(item.value ->> '$.id'                  as integer)    as product_id,
        item.value ->> '$.title'                                   as product_title,
        cast(item.value ->> '$.price'               as double)     as price,
        cast(item.value ->> '$.quantity'            as integer)    as quantity,
        cast(item.value ->> '$.total'               as double)     as line_total,
        cast(item.value ->> '$.discountPercentage'  as double)     as discount_pct,
        cast(item.value ->> '$.discountedTotal'     as double)     as discounted_total,
        source._ingested_at
    from source,
        json_each(source.products) as item
)

select *
from exploded
where cart_id is not null