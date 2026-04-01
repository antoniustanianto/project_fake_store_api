with cart_details as (
    select * from {{ ref('mart_cart_details') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

sales as (
    select
        product_id,
        sum(quantity)                   as total_quantity_sold,
        sum(total_price)                as total_revenue,
        count(distinct cart_id)         as total_orders

    from cart_details
    group by 1
),

final as (
    select
        p.product_id,
        p.title,
        p.category,
        p.price,
        p.rating_rate,
        p.rating_count,

        coalesce(s.total_quantity_sold, 0)  as total_quantity_sold,
        coalesce(s.total_revenue, 0)        as total_revenue,
        coalesce(s.total_orders, 0)         as total_orders

    from products p
    left join sales s using (product_id)
)

select * from final