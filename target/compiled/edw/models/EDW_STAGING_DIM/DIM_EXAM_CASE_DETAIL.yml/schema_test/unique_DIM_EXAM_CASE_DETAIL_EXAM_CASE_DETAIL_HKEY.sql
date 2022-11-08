
    
    



select count(*) as validation_errors
from (

    select
        EXAM_CASE_DETAIL_HKEY

    from EDW_STAGING_DIM.DIM_EXAM_CASE_DETAIL
    where EXAM_CASE_DETAIL_HKEY is not null
    group by EXAM_CASE_DETAIL_HKEY
    having count(*) > 1

) validation_errors


