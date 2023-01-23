






with a as (

    select count(*) as count_a from EDW_STAGING_DIM.DIM_BWC_CALC_DRG_OUTPUT 

    
     where BWC_CALC_DRG_OUTPUT_HKEY not in 
    (
        
            MD5( '99999' )
        
    )
        
),
b as (

    select count(*) as count_b from STAGING.DST_BWC_CALC_DRG_OUTPUT

),
final as (

    select abs(
            (select count_a from a) -
            (select count_b from b)
            )
        as diff_count

)

select diff_count from final
