






with a as (

    select count(*) as count_a from EDW_STAGING_DIM.DIM_POLICY_STANDING 

    
     where POLICY_STANDING_HKEY not in 
    (
        
            MD5( '99999' )
        ,
            MD5( 'BL-N/A-N/A-N' )
        ,
            MD5( 'MIF-N/A-N/A-N' )
        ,
            MD5( 'PA-N/A-N/A-N' )
        ,
            MD5( 'PEC-N/A-N/A-N' )
        ,
            MD5( 'PES-N/A-N/A-N' )
        ,
            MD5( 'SI-N/A-N/A-N' )
        
    )
        
),
b as (

    select count(*) as count_b from STAGING.DSV_POLICY_STANDING

),
final as (

    select abs(
            (select count_a from a) -
            (select count_b from b)
            )
        as diff_count

)

select diff_count from final

