






with a as (

    select count(*) as count_a from EDW_STAGING_DIM.DIM_ICD 

    
     where ICD_HKEY not in 
    (
        
            MD5( '99999' )
        ,
            MD5( '-1111' )
        ,
            MD5( '-2222' )
        
    )
        
            and current_record_ind = 'Y'
),
b as (

    select count(*) as count_b from STAGING.DST_ICD

),
final as (

    select abs(
            (select count_a from a) -
            (select count_b from b)
            )
        as diff_count

)

select diff_count from final
