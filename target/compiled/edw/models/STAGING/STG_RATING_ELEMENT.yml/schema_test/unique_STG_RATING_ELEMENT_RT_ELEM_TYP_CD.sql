
    
    



select count(*) as validation_errors
from (

    select
        RT_ELEM_TYP_CD

    from STAGING.STG_RATING_ELEMENT
    where RT_ELEM_TYP_CD is not null
    group by RT_ELEM_TYP_CD
    having count(*) > 1

) validation_errors


