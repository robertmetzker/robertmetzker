
    
    



select count(*) as validation_errors
from (

    select
        PRICING_TYPE_HKEY

    from EDW_STAGING_DIM.DIM_DRUG_PRICING_TYPE
    where PRICING_TYPE_HKEY is not null
    group by PRICING_TYPE_HKEY
    having count(*) > 1

) validation_errors


