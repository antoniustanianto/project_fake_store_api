with carts as (
    select * from {{ ref('stg_carts') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        -- cart info
        carts.cart_id,
        carts.cart_date,

        -- user info
        carts.user_id,
        users.username,
        users.email,
        users.first_name,
        users.last_name,

        -- product info
        carts.product_id,
        products.title         as product_title,
        products.category      as product_category,
        products.price         as product_price,
        carts.quantity,

        -- total
        products.price * carts.quantity as total_price,

        -- metadata
        carts._extracted_at

    from carts
    left join users using (user_id)
    left join products using (product_id)
)

select * from final