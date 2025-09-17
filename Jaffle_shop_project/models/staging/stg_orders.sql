with source as(
    select * from {{ref("raw_orders")}}
),
rename as (
    select id as order_id, user_id, order_date, status
    from source
)
select * from rename