
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_PRICING_TYPE
where FK_NDCL_CODE is null


