







with left_table as (

  select
    INDUSTRY_GROUP_HKEY as id

  from EDW_STG_CLAIMS_MART.FLF_CLAIM_ICD_HISTORY

  where INDUSTRY_GROUP_HKEY is not null
    and INDUSTRY_GROUP_HKEY not in (  '40c5dea533476acdd01f7ef0e84de22f', 'fcbcdcb8f6b1c597c5fdc7a54cd321ae'  ) 

),

right_table as (

  select
    INDUSTRY_GROUP_HKEY as id

  from EDW_STAGING_DIM.DIM_INDUSTRY_GROUP

  where INDUSTRY_GROUP_HKEY is not null
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

