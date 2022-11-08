
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_CODES
where FK_DCPR_CODE is null


