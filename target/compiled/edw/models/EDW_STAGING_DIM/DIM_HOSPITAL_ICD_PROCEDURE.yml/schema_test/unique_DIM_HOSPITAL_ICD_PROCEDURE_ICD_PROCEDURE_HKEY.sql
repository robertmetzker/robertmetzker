
    
    



select count(*) as validation_errors
from (

    select
        ICD_PROCEDURE_HKEY

    from EDW_STAGING_DIM.DIM_HOSPITAL_ICD_PROCEDURE
    where ICD_PROCEDURE_HKEY is not null
    group by ICD_PROCEDURE_HKEY
    having count(*) > 1

) validation_errors


