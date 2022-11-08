







with left_table as (

  select
    ACTIVITY_TIME_KEY as id

  from EDW_STG_CLAIMS_MART.FLF_CLAIM_ACTIVITY

  where ACTIVITY_TIME_KEY is not null
    and ACTIVITY_TIME_KEY not in (  -1 ) 

),

right_table as (

  select
    TIME_KEY as id

  from EDW_STAGING_DIM.DIM_TIME

  where TIME_KEY is not null
    and 1=1

),

exceptions as (

  select
    left_table.id,
    right_table.id as right_id

  from left_table

  left join right_table
         on left_table.id = right_table.id

  where right_table.id is null

)

select count(*) from exceptions

