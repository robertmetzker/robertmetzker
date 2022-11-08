
    
    



select count(*) as validation_errors
from (

    select
        NDC_11_CODE||FK_MDDI_NMBR||FK_MDDF_CRT_DTTM||MGMD_CRT_DTTM

    from STAGING.STG_MEDISPAN_GPI_GNRC_NM_DRUG_DSCRPTR
    where NDC_11_CODE||FK_MDDI_NMBR||FK_MDDF_CRT_DTTM||MGMD_CRT_DTTM is not null
    group by NDC_11_CODE||FK_MDDI_NMBR||FK_MDDF_CRT_DTTM||MGMD_CRT_DTTM
    having count(*) > 1

) validation_errors


