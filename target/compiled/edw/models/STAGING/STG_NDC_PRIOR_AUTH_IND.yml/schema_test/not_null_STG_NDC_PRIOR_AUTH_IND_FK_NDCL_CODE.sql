
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_PRIOR_AUTH_IND
where FK_NDCL_CODE is null


