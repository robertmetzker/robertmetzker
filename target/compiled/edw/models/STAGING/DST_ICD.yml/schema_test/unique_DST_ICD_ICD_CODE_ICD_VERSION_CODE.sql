
    
    



select count(*) as validation_errors
from (

    select
        ICD_CODE||ICD_VERSION_CODE

    from STAGING.DST_ICD
    where ICD_CODE||ICD_VERSION_CODE is not null
    group by ICD_CODE||ICD_VERSION_CODE
    having count(*) > 1

) validation_errors


