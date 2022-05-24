with blue_chip as (
  select *
  from {{ ref('blue_chip_collection') }}
),

contracts as (
  select distinct address
  from {{ ref('stg_contracts') }}
),

erc721_transfer as (
  select
    contract_address as nft_contract_address,
    tokenid as nft_token_id,
    to as to_address,
    evt_block_time as block_time
  from {{ ref('ERC721_evt_Transfer') }}
),

erc1155_single_transfer as (
  select
    contract_address as nft_contract_address,
    id as nft_token_id,
    to as to_address,
    evt_block_time as block_time
  from {{ ref('ERC1155_evt_TransferSingle') }}
),

erc1155_batch_transfer as (
  select
    contract_address as nft_contract_address,
    explode(ids) as nft_token_id,
    to as to_address,
    evt_block_time as block_time
  from {{ ref('ERC1155_evt_TransferBatch') }}
),

cryptopunks_transfer as (
  select
    contract_address as nft_contract_address,
    punkindex as nft_token_id,
    to as to_address,
    evt_block_time as block_time
  from {{ ref('cryptopunks_CryptoPunksMarket_evt_PunkTransfer') }}
  union distinct
  select
    contract_address as nft_contract_address,
    punkindex as nft_token_id,
    toaddress as to_address,
    evt_block_time as block_time
  from {{ ref('cryptopunks_CryptoPunksMarket_evt_PunkBought') }}
  union distinct
  select
    contract_address as nft_contract_address,
    punkindex as nft_token_id,
    to as to_address,
    evt_block_time as block_time
  from {{ ref('cryptopunks_CryptoPunksMarket_evt_Assign') }}
), 

holder_info as (
  select distinct
    nft_contract_address,
    nft_token_id,
    to_address as holder
  from (
    select
      nft_contract_address,
      nft_token_id,
      to_address,
      row_number()over(partition by nft_contract_address, nft_token_id order by block_time desc) as rank 
    from (
      select * from erc721_transfer
      union
      select * from erc1155_single_transfer
      union
      select * from erc1155_batch_transfer
      union
      select * from cryptopunks_transfer
      where to_address != '0x0000000000000000000000000000000000000000'
    )
  )
  where rank = 1
)

select distinct
  holder_info.holder as address,
  'NFT Blue Chip Holder' as label
  'NFT Collector' as label_type
from blue_chip
join holder_info
  on blue_chip.nft_contract_address = holder_info.nft_contract_address
left anti join contracts
  on holder_info.holder = contracts.address
