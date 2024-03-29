with txn as (
  select
  * from {{ ref("stg_transactions") }}
),

raw_swap as (
  select
  * from {{ ref("metamask_MetamaskSwap_call_swap") }}
),

swap as (
  select
    call_block_number as block_number,
    call_block_time as block_time,
    call_tx_hash as tx_hash,
    dt,
    router,
    token,
    amount
  from raw_swap
  where call_success is True
)

select 
  s.block_number,
  s.block_time,
  s.tx_hash,
  s.dt,
  s.router,
  s.token,
  s.amount,
  t.from_address as from
from txn as t
inner join swap as s
  on s.block_number = t.block_number
  and s.tx_hash = t.hash
