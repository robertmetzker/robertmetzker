
    
    



select count(*) as validation_errors
from STAGING.STG_ICD_CHAPTER_BODY_PART_REFERENCE
where ICD_VRSN_CODE is null


