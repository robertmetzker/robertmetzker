
    
    



select count(*) as validation_errors
from STAGING.STG_MEDISPAN_DRUG_DSCRPTR_IDNTFR_NTNL_DRUG_CD_VER
where MDNV_CRT_DTTM is null


