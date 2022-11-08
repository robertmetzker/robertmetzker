
    
    



select count(*) as validation_errors
from STAGING.STG_MEDISPAN_NM_DRUG_DSCRPTR
where MDPN_CRT_DTTM is null


