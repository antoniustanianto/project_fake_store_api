with product_summary as (
    select * from {{ ref('mart_product_summary') }}
),

final as (
    select
        category,

        count(product_id)               as total_products,
        avg(price)                      as avg_price,
        avg(rating_rate)                as avg_rating,
        sum(total_quantity_sold)        as total_quantity_sold,
        sum(total_revenue)              as total_revenue,
        sum(total_orders)               as total_orders

    from product_summary
    group by 1
)

select * from final