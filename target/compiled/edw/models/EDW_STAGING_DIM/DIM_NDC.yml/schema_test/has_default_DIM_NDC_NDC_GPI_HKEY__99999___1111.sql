

with meet_condition as (

    select count( NDC_GPI_HKEY ) as matches from EDW_STAGING_DIM.DIM_NDC
    where PRIMARY_SOURCE_SYSTEM = 'MANUAL ENTRY' 
    and NDC_GPI_HKEY in (
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

