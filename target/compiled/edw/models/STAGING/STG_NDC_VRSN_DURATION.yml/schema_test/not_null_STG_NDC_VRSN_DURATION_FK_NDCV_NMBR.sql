
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_VRSN_DURATION
where FK_NDCV_NMBR is null


