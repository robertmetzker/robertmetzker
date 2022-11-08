
    
    



select count(*) as validation_errors
from STAGING.DST_ICD
where ICD_CODE||ICD_VERSION_CODE is null


