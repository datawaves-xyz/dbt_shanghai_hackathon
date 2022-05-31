with ens_name (
  select
  * from {{ source("ethereum_property_materials", "phishing") }}
)

select
  address
  'phishing' as label
from ens_name