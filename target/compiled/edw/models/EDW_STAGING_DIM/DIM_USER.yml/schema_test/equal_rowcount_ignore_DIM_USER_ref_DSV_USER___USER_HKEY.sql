






with a as (

    select count(*) as count_a from EDW_STAGING_DIM.DIM_USER 

    
),
b as (

    select count(*) as count_b from STAGING.DSV_USER

),
final as (

    select abs(
            (select count_a from a) -
            (select count_b from b)
            )
        as diff_count

)

select diff_count from final

