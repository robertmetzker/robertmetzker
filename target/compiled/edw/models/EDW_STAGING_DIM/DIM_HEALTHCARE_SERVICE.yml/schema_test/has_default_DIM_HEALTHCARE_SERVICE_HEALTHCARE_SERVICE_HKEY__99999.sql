

with meet_condition as (

    select count( HEALTHCARE_SERVICE_HKEY ) as matches from EDW_STAGING_DIM.DIM_HEALTHCARE_SERVICE
    where PRIMARY_SOURCE_SYSTEM = 'MANUAL ENTRY' 
    and HEALTHCARE_SERVICE_HKEY in (
        MD5( '99999' )
        
    )
),
equal_cnt as (
    select ( case when MATCHES = 1
     then 0 else 
          (case when matches = 0 then 1 else matches end)
     end ) as test
    from meet_condition
 )
select * from equal_cnt

