with opensea_trades as (
  select *
  from {{ ref('nft_trades') }}
  where platform = 'OpenSea'
)

select distinct
  address,
  'OpenSea Trader' as label,
  'NFT Collector' as label_type
from (
  select distinct seller as address
  from opensea_trades
  union
  select distinct buyer as address
  from opensea_trades
)
