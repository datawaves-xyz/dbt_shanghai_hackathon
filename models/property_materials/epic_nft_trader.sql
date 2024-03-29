with nft_trades as (
  select *
  from {{ ref('nft_trades') }}
),

contracts as (
  select distinct address
  from {{ ref('stg_contracts') }}
),

trade_info as (
  select
    a.address,
    count(1) as transaction
  from (
    select
      seller as address,
      block_time,
      'sell' as trade_type
    from nft_trades
    union all
    select
      buyer as address,
      block_time,
      'buy' as trade_type
    from nft_trades
  ) as a
  left anti join contracts
  on a.address = contracts.address
  group by a.address
),

top_stat as (
  select round(percentile(transaction, 0.99), 0) as target_value
  from trade_info 
)

select distinct
  trade_info.address,
  'Epic NFT Trader' as label
from trade_info
full outer join top_stat
where trade_info.transaction >= top_stat.target_value
