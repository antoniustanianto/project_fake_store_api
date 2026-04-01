with source as (
    select * from {{ source('fakestore_raw', 'products') }}
),

renamed as (
    select
        id                          as product_id,
        title,
        price,
        description,
        category,
        image                       as image_url,

        -- flatten nested rating
        rating.rate                 as rating_rate,
        rating.count                as rating_count,

        -- metadata
        _extracted_at,
        _source,
        _dag_run_id

    from source
)

select * from renamed