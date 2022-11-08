
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_MODIFIER_SEQUENCE
where MODIFIER_SEQUENCE_CODE_HKEY is null


