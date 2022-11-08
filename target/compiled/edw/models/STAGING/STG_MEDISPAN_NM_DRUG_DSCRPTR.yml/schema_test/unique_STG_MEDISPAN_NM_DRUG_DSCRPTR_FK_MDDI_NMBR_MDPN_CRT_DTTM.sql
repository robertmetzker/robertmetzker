
    
    



select count(*) as validation_errors
from (

    select
        FK_MDDI_NMBR||MDPN_CRT_DTTM

    from STAGING.STG_MEDISPAN_NM_DRUG_DSCRPTR
    where FK_MDDI_NMBR||MDPN_CRT_DTTM is not null
    group by FK_MDDI_NMBR||MDPN_CRT_DTTM
    having count(*) > 1

) validation_errors


