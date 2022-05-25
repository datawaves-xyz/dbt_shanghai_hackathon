with smart_nft_traders (
  select
  * from {{ ref("smart_nft_traders") }}
)

select * from smart_nft_traders