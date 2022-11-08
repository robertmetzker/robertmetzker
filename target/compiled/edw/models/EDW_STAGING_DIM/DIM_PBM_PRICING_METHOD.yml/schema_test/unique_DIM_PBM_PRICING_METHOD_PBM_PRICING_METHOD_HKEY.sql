
    
    



select count(*) as validation_errors
from (

    select
        PBM_PRICING_METHOD_HKEY

    from EDW_STAGING_DIM.DIM_PBM_PRICING_METHOD
    where PBM_PRICING_METHOD_HKEY is not null
    group by PBM_PRICING_METHOD_HKEY
    having count(*) > 1

) validation_errors


