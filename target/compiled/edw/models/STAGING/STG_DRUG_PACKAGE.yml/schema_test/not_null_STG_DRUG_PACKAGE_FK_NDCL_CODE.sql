
    
    



select count(*) as validation_errors
from STAGING.STG_DRUG_PACKAGE
where FK_NDCL_CODE is null


