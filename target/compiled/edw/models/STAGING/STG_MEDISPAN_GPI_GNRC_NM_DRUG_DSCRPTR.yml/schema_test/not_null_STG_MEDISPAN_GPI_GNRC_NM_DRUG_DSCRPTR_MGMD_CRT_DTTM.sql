
    
    



select count(*) as validation_errors
from STAGING.STG_MEDISPAN_GPI_GNRC_NM_DRUG_DSCRPTR
where MGMD_CRT_DTTM is null


