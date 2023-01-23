





with a as (

    select count(*) as count_a from EDW_STAGING_DIM.DIM_DRUG_PRICING_TYPE 
    
 
        where PRICING_TYPE_HKEY not in ( MD5('99999'), MD5('-1111'), MD5('-2222'), MD5('88888'), MD5('00000') )  
  
        
),
b as (

    select count(*) as count_b from STAGING.DST_DRUG_PRICING_TYPE

),
final as (

    select abs(
            (select count_a from a) -
            (select count_b from b)
            )
        as diff_count

)

select diff_count from final
