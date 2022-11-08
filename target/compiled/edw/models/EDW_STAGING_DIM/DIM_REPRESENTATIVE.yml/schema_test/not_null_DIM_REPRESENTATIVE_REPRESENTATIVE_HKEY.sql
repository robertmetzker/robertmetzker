
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_REPRESENTATIVE
where REPRESENTATIVE_HKEY is null


