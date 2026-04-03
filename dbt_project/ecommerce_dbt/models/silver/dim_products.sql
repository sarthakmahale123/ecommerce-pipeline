with source as (
    select * from {{ source('bronze', 'products_raw') }}
),

flattened as (
    select
        id                                                          as product_id,
        title,
        description,
        category,
        brand,
        sku,
        price,
        discount_percentage,
        round(price * (1 - discount_percentage / 100), 2)          as discounted_price,
        rating,
        stock,
        availability_status,
        warranty_information,
        shipping_information,
        return_policy,
        minimum_order_qty,
        weight,
        thumbnail,

        -- flatten dimensions JSON object into individual columns
        cast(json_extract(dimensions, '$.width')  as double)       as dim_width,
        cast(json_extract(dimensions, '$.height') as double)       as dim_height,
        cast(json_extract(dimensions, '$.depth')  as double)       as dim_depth,

        _ingested_at

    from source
    where id is not null
),

deduped as (
    select *,
        row_number() over (
            partition by product_id
            order by _ingested_at desc
        ) as rn
    from flattened
)

select * exclude (rn)
from deduped
where rn = 1