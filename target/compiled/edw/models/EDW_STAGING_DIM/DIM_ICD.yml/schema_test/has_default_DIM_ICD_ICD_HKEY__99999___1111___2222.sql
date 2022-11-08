

with meet_condition as (

    select count( ICD_HKEY ) as matches from EDW_STAGING_DIM.DIM_ICD
    where PRIMARY_SOURCE_SYSTEM = 'MANUAL ENTRY' 
    and ICD_HKEY in (
        MD5( '99999' )
        ,MD5( '-1111' )
        ,MD5( '-2222' )
        
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

