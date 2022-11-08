
    
    



select count(*) as validation_errors
from STAGING.STG_DRUG_PACKAGE
where NVMG_CRT_DTTM is null


