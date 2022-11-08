
    
    



select count(*) as validation_errors
from (

    select
        INVOICE_NUMBER|| LINE_SEQUENCE|| EDIT_CODE|| EOB_CODE|| LINE_EDIT_SOURCE|| LINE_EOB_SOURCE|| LINE_EDIT_PHASE_APPLIED|| LINE_EOB_PHASE_APPLIED|| EOB_ENTRY_DATE|| EDIT_ENTRY_DATE

    from STAGING.DST_MEDICAL_INVOICE_LINE_EDIT_EOB
    where INVOICE_NUMBER|| LINE_SEQUENCE|| EDIT_CODE|| EOB_CODE|| LINE_EDIT_SOURCE|| LINE_EOB_SOURCE|| LINE_EDIT_PHASE_APPLIED|| LINE_EOB_PHASE_APPLIED|| EOB_ENTRY_DATE|| EDIT_ENTRY_DATE is not null
    group by INVOICE_NUMBER|| LINE_SEQUENCE|| EDIT_CODE|| EOB_CODE|| LINE_EDIT_SOURCE|| LINE_EOB_SOURCE|| LINE_EDIT_PHASE_APPLIED|| LINE_EOB_PHASE_APPLIED|| EOB_ENTRY_DATE|| EDIT_ENTRY_DATE
    having count(*) > 1

) validation_errors


