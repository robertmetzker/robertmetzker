
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_PRICING_TYPE
where FK_DCPR_CODE is null


