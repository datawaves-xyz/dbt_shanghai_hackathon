with ens_name (
  select
  * from {{ ref("phishing") }}
)

select
  address
  'phishing' as label
from ens_name