
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_RATING_ELEMENT
where RATING_ELEMENT_HKEY is null


