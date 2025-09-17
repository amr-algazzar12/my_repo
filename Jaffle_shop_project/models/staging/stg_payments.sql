with source as (
    select * from {{ref("raw_payments")}}
),
rename as (
    select id as payment_id, order_id, payment_method, amount / 100 as amount
    from source
)
select * from rename