
    
    



select count(*) as validation_errors
from STAGING.STG_MEDISPAN_DRUG_DSCRPTR_IDNTFR_NTNL_DRUG_CD_VER
where FK_MDDI_NMBR is null


