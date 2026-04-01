with source as (
    select * from {{ source('fakestore_raw', 'users') }}
),

renamed as (
    select
        id                          as user_id,
        email,
        username,
        phone,

        -- flatten nested name
        name.firstname              as first_name,
        name.lastname               as last_name,

        -- flatten nested address
        address.street              as address_street,
        address.city                as address_city,
        address.zipcode             as address_zipcode,
        address.number              as address_number,

        -- metadata
        _extracted_at,
        _source,
        _dag_run_id

    from source
)

select * from renamed