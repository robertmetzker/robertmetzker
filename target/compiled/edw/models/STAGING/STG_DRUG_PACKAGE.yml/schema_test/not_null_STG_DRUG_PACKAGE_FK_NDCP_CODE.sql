
    
    



select count(*) as validation_errors
from STAGING.STG_DRUG_PACKAGE
where FK_NDCP_CODE is null


