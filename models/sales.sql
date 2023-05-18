with sales as (
    select
        order_date,
        sum(credit_card_amount) as credit_card_sales,
        sum(coupon_amount) as coupon_sales,
        sum(bank_transfer_amount) as bank_transfer_sales,
        sum(gift_card_amount) as gift_card_sales,
        sum(amount) as total_sales
    from {{ ref('orders') }}
    group by order_date
)

select * from sales