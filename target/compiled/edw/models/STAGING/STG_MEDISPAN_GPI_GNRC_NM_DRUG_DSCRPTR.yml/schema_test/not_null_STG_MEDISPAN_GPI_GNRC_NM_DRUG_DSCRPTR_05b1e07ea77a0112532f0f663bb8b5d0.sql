
    
    



select count(*) as validation_errors
from STAGING.STG_MEDISPAN_GPI_GNRC_NM_DRUG_DSCRPTR
where NDC_11_CODE||FK_MDDI_NMBR||FK_MDDF_CRT_DTTM||MGMD_CRT_DTTM is null


