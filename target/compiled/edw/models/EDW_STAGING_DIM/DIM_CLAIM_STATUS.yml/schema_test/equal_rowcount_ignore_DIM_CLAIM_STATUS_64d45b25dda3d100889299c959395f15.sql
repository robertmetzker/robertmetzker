






with a as (

    select count(*) as count_a from EDW_STAGING_DIM.DIM_CLAIM_STATUS 

    
     where CLAIM_STATUS_HKEY not in 
    (
        
            MD5( '99999' )
        ,
            MD5( '-1111' )
        
    )
        
),
b as (

    select count(*) as count_b from STAGING.DST_CLAIM_STATUS

),
final as (

    select abs(
            (select count_a from a) -
            (select count_b from b)
            )
        as diff_count

)

select diff_count from final

