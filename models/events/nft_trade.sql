with nft_trades as (
  select *
  from {{ ref('nft_trades') }}
)

select
  platform,
  erc_standard,
  trade_type,
  nft_conract_address,
  nft_project_name,
  nft_token_id,
  eth_amount,
  usd_amount,
  buyer,
  seller,
  block_time,
  cast(to_date(block_time) as string) as dt
from nft_trades
