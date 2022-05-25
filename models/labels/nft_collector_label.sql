with diversified_nft_holder as (
  select
  * from {{ ref("diversified_nft_holder") }}
),

epic_nft_trader as (
  select
  * from {{ ref("epic_nft_trader") }}
),

legendary_nft_trader as (
  select
  * from {{ ref("legendary_nft_trader") }}
),

blue_chip_holder as (
  select
  * from {{ ref("nft_blue_chip_holder") }}
),

opensea_trader as (
  select
  * from {{ ref("opensea_trader") }}
),

rare_nft_trader as (
  select
  * from {{ ref("rare_nft_trader") }}
),

uncommon_nft_trader as (
  select
  * from {{ ref("uncommon_nft_trader") }}
)

select * from diversified_nft_holder
union
select * from epic_nft_trader
union
select * from legendary_nft_trader
union
select * from blue_chip_holder
union
select * from opensea_trader
union
select * from rare_nft_trader
union
select * from uncommon_nft_trader
