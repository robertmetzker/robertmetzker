
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_EMPLOYER
where EMPLOYER_HKEY is null


