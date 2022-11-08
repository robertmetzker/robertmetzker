
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_EXAM_CASE_DETAIL
where EXAM_CASE_DETAIL_HKEY is null


