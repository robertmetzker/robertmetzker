
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_YEAR_CONTROL_ELEMENT
where YEAR_CONTROL_ELEMENT_HKEY is null


