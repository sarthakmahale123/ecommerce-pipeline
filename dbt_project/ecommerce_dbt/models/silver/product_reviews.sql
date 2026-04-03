with source as (
    select
        id          as product_id,
        title       as product_title,
        category,
        reviews,
        _ingested_at
    from {{ source('bronze', 'products_raw') }}
    where reviews is not null
      and reviews != '[]'
),

exploded as (
    select
        source.product_id,
        source.product_title,
        source.category,
        cast(review.value ->> '$.rating'       as integer)     as rating,
        review.value ->> '$.comment'                           as comment,
        cast(review.value ->> '$.date'         as timestamp)   as review_date,
        review.value ->> '$.reviewerName'                      as reviewer_name,
        source._ingested_at
    from source,
        json_each(source.reviews) as review
)

select *
from exploded
where product_id is not null