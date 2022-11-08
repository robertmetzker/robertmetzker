
    
    



select count(*) as validation_errors
from (

    select
        MODIFIER_SEQUENCE_CODE_HKEY

    from EDW_STAGING_DIM.DIM_MODIFIER_SEQUENCE
    where MODIFIER_SEQUENCE_CODE_HKEY is not null
    group by MODIFIER_SEQUENCE_CODE_HKEY
    having count(*) > 1

) validation_errors


