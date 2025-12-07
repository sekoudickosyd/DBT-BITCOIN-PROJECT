
{{ 
    config(
        materialized='incremental',
        incremental_strategy='append'
    )   
}}

with flattened as (

    select 
        tx.hash_key,
        tx.block_number,
        tx.block_timestamp,
        tx.is_coinbase,
        f.value:address::STRING as address,
        f.value:value::FLOAT as output_value

    from {{ ref('stg_btc') }} tx,

    LATERAL FLATTEN(input => outputs) f

    where f.value:address IS NOT NULL

    {% if is_incremental() %}

    and tx.block_timestamp >= (select max(block_timestamp) from {{ this }})

    {% endif %}
)

select 
    hash_key,
    block_number,
    block_timestamp,
    is_coinbase,
    address,
    output_value

from flattened