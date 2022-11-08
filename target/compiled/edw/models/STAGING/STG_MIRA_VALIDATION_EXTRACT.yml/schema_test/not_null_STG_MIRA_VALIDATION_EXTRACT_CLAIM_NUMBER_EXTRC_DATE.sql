
    
    



select count(*) as validation_errors
from STAGING.STG_MIRA_VALIDATION_EXTRACT
where CLAIM_NUMBER||EXTRC_DATE is null


