
    with source as (
        select * from {{source('jaffle_shop_raw', 'orders')}}
    ),

    renamed as (
        select
            id,
        user_id,
        order_date,
        status,
        _etl_loaded_at
        from source
    )

    select * from renamed
    