





with validation_errors as (

    select
        ACTION_TYPE, ACTIVITY_NAME_TYPE, ACTIVITY_CONTEXT_TYPE_NAME, ACTIVITY_SUBCONTEXT_TYPE_NAME, PROCESS_AREA, USER_FUNCTIONAL_ROLE_DESC
    from EDW_STAGING_DIM.DIM_ACTIVITY

    group by ACTION_TYPE, ACTIVITY_NAME_TYPE, ACTIVITY_CONTEXT_TYPE_NAME, ACTIVITY_SUBCONTEXT_TYPE_NAME, PROCESS_AREA, USER_FUNCTIONAL_ROLE_DESC
    having count(*) > 1

)

select count(*)
from validation_errors

