with source as (
    select * from {{ source('bronze', 'users_raw') }}
),

flattened as (
    select
        id                                                          as user_id,
        first_name,
        last_name,
        first_name || ' ' || last_name                             as full_name,
        age,
        gender,
        email,
        phone,
        username,
        try_cast(birth_date as date)                               as birth_date,
        blood_group,
        height,
        weight,
        eye_color,
        role,
        university,

        -- flatten hair JSON
        json_extract_string(hair, '$.color')                       as hair_color,
        json_extract_string(hair, '$.type')                        as hair_type,

        -- flatten address JSON
        json_extract_string(address, '$.city')                     as city,
        json_extract_string(address, '$.state')                    as state,
        json_extract_string(address, '$.country')                  as country,
        json_extract_string(address, '$.postalCode')               as postal_code,

        -- flatten company JSON
        json_extract_string(company, '$.name')                     as company_name,
        json_extract_string(company, '$.department')               as department,
        json_extract_string(company, '$.title')                    as job_title,

        _ingested_at

        -- ssn and ein deliberately excluded — PII stays in Bronze only

    from source
    where id is not null
),

deduped as (
    select *,
        row_number() over (
            partition by user_id
            order by _ingested_at desc
        ) as rn
    from flattened
)

select * exclude (rn)
from deduped
where rn = 1