with ens_name_reverse (
  select
  * from {{ ref("ens_name_reverse") }}
)

select * from ens_name_reverse