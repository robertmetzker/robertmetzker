
    
    



select count(*) as validation_errors
from (

    select
        ICD_CODE||'-'||ICD_VRSN_CODE

    from STAGING.STG_ICD_CHAPTER_BODY_PART_REFERENCE
    where ICD_CODE||'-'||ICD_VRSN_CODE is not null
    group by ICD_CODE||'-'||ICD_VRSN_CODE
    having count(*) > 1

) validation_errors


