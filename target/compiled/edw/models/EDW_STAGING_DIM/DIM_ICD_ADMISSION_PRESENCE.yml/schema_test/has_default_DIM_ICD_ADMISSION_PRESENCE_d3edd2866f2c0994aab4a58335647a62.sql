

with meet_condition as (

    select count( PRESENT_ON_ADMISSION_HKEY ) as matches from EDW_STAGING_DIM.DIM_ICD_ADMISSION_PRESENCE
    where PRIMARY_SOURCE_SYSTEM = 'MANUAL ENTRY' 
    and PRESENT_ON_ADMISSION_HKEY in (
        MD5( '99999' )
        ,MD5( '-1111' )
        
    )
),
equal_cnt as (
    select ( case when MATCHES = 2
     then 0 else 
          (case when matches = 0 then 1 else matches end)
     end ) as test
    from meet_condition
 )
select * from equal_cnt

