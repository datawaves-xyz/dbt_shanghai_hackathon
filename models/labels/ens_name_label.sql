with ens_name (
  select
  * from {{ ref("ens_name") }}
)

select * from ens_name