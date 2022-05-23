with erc721_transfer from(
  select *
  from {{ ref('ERC721_evt_Transfer') }}
),

erc1155_single_transfer from (
  select *
  from {{ ref('ERC1155_evt_TransferSingle') }}
),

erc1155_batch_transfer from (
  select *
  from {{ ref('ERC1155_evt_TransferBatch') }}
),

721_transfer from (
  select
    evt_block_number,
    evt_block_time,
    evt_index,
    evt_tx_hash,
    contract_address,
    from,
    to,
    array(tokenId) as token_ids,
    'ERC721' as type,
    dt
  from erc721_transfer where from='0x0000000000000000000000000000000000000000' 
),

1155_transfer from (
  select
    evt_block_number,
    evt_block_time,
    evt_index,
    evt_tx_hash,
    contract_address,
    from,
    to,
    array(id) as token_ids,
    'ERC1155' as type,
    dt
from erc1155_single_transfer where from='0x0000000000000000000000000000000000000000' 

union

select
    evt_block_number,
    evt_block_time,
    evt_index,
    evt_tx_hash,
    contract_address,
    from,
    to,
    ids as token_ids,
    'ERC1155' as type,
    dt
from erc1155_batch_transfer where from='0x0000000000000000000000000000000000000000'
)

select * from 721_transfer
union
select * from 1155_transfer
