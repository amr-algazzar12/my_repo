with customers as (
    select * from {{ref ("stg_customers")}}
),
orders as (
    select * from {{ref("stg_orders")}}
),
payments as (
    select * from {{ref("stg_payments")}}
),
customer_orders as(
    select user_id as customer_id,
           min(order_date) as first_order,
           max(order_date) as latest_order,
           count(order_id) as total_orders
    from orders
    group by customer_id
),
customer_payments as(
    select user_id as customer_id,
           sum(amount) as total_amount
    from payments
    left join orders on payments.order_id = orders.order_id
    group by customer_id
),
final as(
    select customers.customer_id,
           first_order,
           latest_order,
           total_orders,
           total_amount
    from customers
    left join customer_orders on customers.customer_id = customer_orders.customer_id
    left join customer_payments on customers.customer_id = customer_payments.customer_id
)
select * from final