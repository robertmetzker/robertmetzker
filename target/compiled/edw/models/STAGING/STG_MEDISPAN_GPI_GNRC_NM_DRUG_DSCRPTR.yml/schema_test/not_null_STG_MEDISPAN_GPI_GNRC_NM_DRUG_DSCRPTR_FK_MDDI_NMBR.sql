
    
    



select count(*) as validation_errors
from STAGING.STG_MEDISPAN_GPI_GNRC_NM_DRUG_DSCRPTR
where FK_MDDI_NMBR is null


