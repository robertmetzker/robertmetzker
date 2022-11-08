
    
    



select count(*) as validation_errors
from STAGING.STG_DRG
where DRG_CODE||EFFECTIVE_DATE is null


