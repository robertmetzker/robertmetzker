
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_PRICING_TYPE
where FK_NDCP_CODE is null


