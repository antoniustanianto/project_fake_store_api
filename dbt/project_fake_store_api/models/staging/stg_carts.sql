with source as (
    select * from {{ source('fakestore_raw', 'carts') }}
),

unnested as (
    select
        id                          as cart_id,
        userId                      as user_id,
        date                        as cart_date,

        -- unnest array of products
        p.productId                 as product_id,
        p.quantity                  as quantity,

        -- metadata
        _extracted_at,
        _source,
        _dag_run_id

    from source,
    unnest(products) as p
)

select * from unnested