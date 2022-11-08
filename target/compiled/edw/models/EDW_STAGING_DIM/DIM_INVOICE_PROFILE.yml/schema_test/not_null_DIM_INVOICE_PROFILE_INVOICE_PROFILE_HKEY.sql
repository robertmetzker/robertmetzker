
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_INVOICE_PROFILE
where INVOICE_PROFILE_HKEY is null


