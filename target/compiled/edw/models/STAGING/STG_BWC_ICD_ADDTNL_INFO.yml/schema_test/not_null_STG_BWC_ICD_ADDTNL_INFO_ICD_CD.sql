
    
    



select count(*) as validation_errors
from STAGING.STG_BWC_ICD_ADDTNL_INFO
where ICD_CD is null


