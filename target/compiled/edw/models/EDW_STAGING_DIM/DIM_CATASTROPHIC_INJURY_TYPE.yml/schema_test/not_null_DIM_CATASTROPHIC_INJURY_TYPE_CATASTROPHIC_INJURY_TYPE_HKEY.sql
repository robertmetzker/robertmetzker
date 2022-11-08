
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CATASTROPHIC_INJURY_TYPE
where CATASTROPHIC_INJURY_TYPE_HKEY is null


