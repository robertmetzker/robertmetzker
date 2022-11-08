
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_OTC_OR_RX
where FK_NDCV_NMBR is null


