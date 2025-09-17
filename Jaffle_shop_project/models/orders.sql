{% set paymentss = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}
with orders as (
    select * from {{ ref("stg_orders") }}
),

payments as (
    select * from {{ ref("stg_payments") }}
),

order_payments as (
    select 
        order_id,
        {% for payment in paymentss -%}
        sum(case when payment_method = '{{ payment }}' then amount else 0 end) as {{ payment }}_amount,
        {% endfor %}
        -- sum(case when payment_method = 'credit_card' then amount else 0 end) as credit_card_amount,
        -- sum(case when payment_method = 'coupon' then amount else 0 end) as coupon_amount,
        -- sum(case when payment_method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount,
        -- sum(case when payment_method = 'gift_card' then amount else 0 end) as gift_card_amount,
        sum(amount) as total_amount
    from payments
    group by order_id
),

final as (
    select 
        orders.order_id,
        orders.user_id as customer_id,
        orders.order_date,
        orders.status,
        {% for payment in paymentss -%}
        order_payments.{{ payment }}_amount,
        {% endfor %}
        -- order_payments.credit_card_amount,
        -- order_payments.coupon_amount,
        -- order_payments.bank_transfer_amount,
        -- order_payments.gift_card_amount,
        order_payments.total_amount
    from orders
    left join order_payments 
        on orders.order_id = order_payments.order_id
)

select * from final