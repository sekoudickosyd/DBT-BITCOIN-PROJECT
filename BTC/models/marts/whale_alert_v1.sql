with whale as (
    select 
        address,
        sum(output_value) as total_sent,
        count(*) as tx_count

    from {{ ref('stg_btc_transactions') }}

    where output_value > 10 

    group by address 

    order by total_sent desc 
)

select 
    address,
    total_sent,
    tx_count,
    {{ convert_to_usd('total_sent') }} as total_sent_usd

from whale 

order by total_sent desc 