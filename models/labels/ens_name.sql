with name_registered_1 as (
  select
  * from {{ ref("ens_ETHRegistrarController_evt_NameRegistered") }}
),

name_registered_2 as (
  select
  * from {{ ref("ens_ETHRegistrarController2_evt_NameRegistered") }}
),

name_registered_3 as (
  select
  * from {{ ref("ens_ETHRegistrarController3_evt_NameRegistered") }}
),

ens_label_1 as (
  select
    owner AS address,
    lower(name) AS label,
    'ens name' AS label_type
  from name_registered_1
),

ens_label_2 as (
  select
    owner AS address,
    lower(name) AS label,
    'ens name' AS label_type
  from name_registered_2
),

ens_label_3 as (
  select
    owner AS address,
    lower(name) AS label,
    'ens name' AS label_type
  from name_registered_3
)

select * from ens_label_1
union
select * from ens_label_2
union
select * from ens_label_3
