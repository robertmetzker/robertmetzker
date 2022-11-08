
    
    



select count(*) as validation_errors
from STAGING.STG_DRUG_PACKAGE
where FK_DCPR_CODE is null


