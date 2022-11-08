
    
    




with all_values as (

    select distinct
        ASGN_LD_BAL_RL_TYP_CD as value_field

    from STAGING.STG_ASSIGNMENT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'CREA_DISTR','DFLT_DISTR','DIS_PROP_DISTR','FNCT_RL_DISTR','LVL_DISTR','PLCY_RR_DISTR','RND_RBN_DISTR','SPFC_DISTR','USER_CNTX_DISTR'
    )
)

select count(*) as validation_errors
from validation_errors


