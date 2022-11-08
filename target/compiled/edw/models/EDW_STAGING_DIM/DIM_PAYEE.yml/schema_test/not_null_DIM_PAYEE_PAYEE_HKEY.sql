
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PAYEE
where PAYEE_HKEY is null


