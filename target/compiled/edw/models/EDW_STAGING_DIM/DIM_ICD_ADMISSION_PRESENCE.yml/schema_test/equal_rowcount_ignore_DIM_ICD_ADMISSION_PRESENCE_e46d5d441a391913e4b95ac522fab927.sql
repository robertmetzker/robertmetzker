






with a as (

    select count(*) as count_a from EDW_STAGING_DIM.DIM_ICD_ADMISSION_PRESENCE 

    
     where PRESENT_ON_ADMISSION_HKEY not in 
    (
        
            MD5( '99999' )
        ,
            MD5( '-1111' )
        
    )
        
),
b as (

    select count(*) as count_b from STAGING.DST_ICD_ADMISSION_PRESENCE

),
final as (

    select abs(
            (select count_a from a) -
            (select count_b from b)
            )
        as diff_count

)

select diff_count from final

