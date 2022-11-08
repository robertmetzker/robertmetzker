






with a as (

    select count(*) as count_a from EDW_STAGING_DIM.DIM_POLICY_STANDING 

    
     where POLICY_STANDING_HKEY not in 
    (
        
            MD5( '99999' )
        ,
            MD5( '99991' )
        ,
            MD5( '99992' )
        ,
            MD5( '99993' )
        ,
            MD5( '99994' )
        ,
            MD5( '99995' )
        ,
            MD5( '99996' )
        
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

