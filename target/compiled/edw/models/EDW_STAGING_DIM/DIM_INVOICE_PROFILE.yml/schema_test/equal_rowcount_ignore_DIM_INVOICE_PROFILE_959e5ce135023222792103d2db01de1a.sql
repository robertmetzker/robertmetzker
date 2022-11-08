






with a as (

    select count(*) as count_a from EDW_STAGING_DIM.DIM_INVOICE_PROFILE 

    
     where INVOICE_PROFILE_HKEY not in 
    (
        
            MD5( '99999' )
        
    )
        
),
b as (

    select count(*) as count_b from STAGING.DST_INVOICE_PROFILE

),
final as (

    select abs(
            (select count_a from a) -
            (select count_b from b)
            )
        as diff_count

)

select diff_count from final

