
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_MANUFACTURERS
where FK_NDCV_NMBR is null


