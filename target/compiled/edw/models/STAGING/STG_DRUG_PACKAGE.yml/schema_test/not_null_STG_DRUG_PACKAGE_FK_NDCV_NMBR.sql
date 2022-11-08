
    
    



select count(*) as validation_errors
from STAGING.STG_DRUG_PACKAGE
where FK_NDCV_NMBR is null


