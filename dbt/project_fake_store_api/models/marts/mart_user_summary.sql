with cart_details as (
    select * from {{ ref('mart_cart_details') }}
),

final as (
    select
        user_id,
        username,
        email,
        first_name,
        last_name,

        count(distinct cart_id)         as total_carts,
        count(product_id)               as total_items_purchased,
        sum(quantity)                   as total_quantity,
        sum(total_price)                as total_spending,
        avg(total_price)                as avg_spend_per_item,
        min(cart_date)                  as first_purchase_date,
        max(cart_date)                  as last_purchase_date

    from cart_details
    group by 1, 2, 3, 4, 5
)

select * from final