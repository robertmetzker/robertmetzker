
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_CODES
where FK_NDCV_NMBR is null


