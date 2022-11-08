
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_ORGANIZATIONAL_UNIT
where ORGANIZATION_UNIT_HKEY is null


