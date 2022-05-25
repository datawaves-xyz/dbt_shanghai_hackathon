with nft_whales (
  select
  * from {{ ref("nft_whales") }}
)

select * from nft_whales