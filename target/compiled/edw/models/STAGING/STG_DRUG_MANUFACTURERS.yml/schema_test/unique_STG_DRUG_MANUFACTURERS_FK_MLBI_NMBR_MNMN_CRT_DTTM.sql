
    
    



select count(*) as validation_errors
from (

    select
        FK_MLBI_NMBR||MNMN_CRT_DTTM

    from STAGING.STG_DRUG_MANUFACTURERS
    where FK_MLBI_NMBR||MNMN_CRT_DTTM is not null
    group by FK_MLBI_NMBR||MNMN_CRT_DTTM
    having count(*) > 1

) validation_errors


