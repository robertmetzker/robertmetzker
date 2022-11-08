

with meet_condition as (

    select count( YEAR_MONTH_SKEY ) as matches from EDW_STAGING_DIM.DIM_YEAR_MONTH
    where PRIMARY_SOURCE_SYSTEM = 'MANUAL ENTRY' 
    and YEAR_MONTH_SKEY in (
        '-1','-2','-3'
    )
),
equal_cnt as (
    select ( case when MATCHES = 3
     then 0 else 
          (case when matches = 0 then 1 else matches end)
     end ) as test
    from meet_condition
 )
select * from equal_cnt

