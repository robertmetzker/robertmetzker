select current_ind, status, count(*) from dev_edw_32600145.public.CLAIM_PARTICIPATION_BNFCY_DEP_VW
group by 1,2 order by 1,2;

create or replace view  dev_edw_32600145.public.CLAIM_PARTICIPATION_BNFCY_DEP_VW as (
    with mx as (select max(bwc_dw_load_key) as max_load_key 
                from INC_DEV_SOURCE.PCMP.CLAIM_PARTICIPATION_BNFCY_DEP
               ),
    tst as (
    select 
        case when  bwc_dw_last_seen = max_load_key then 'Y' end as CURRENT_IND
        ,case when nvl(current_ind,'X') != 'Y' then 'D' 
              when bwc_dw_last_seen >  bwc_dw_load_key  then 'U' 
              when bwc_dw_load_key = bwc_dw_last_seen then 'I'
             end as STATUS 
        ,* from INC_DEV_SOURCE.PCMP.CLAIM_PARTICIPATION_BNFCY_DEP
        left join mx on (bwc_dw_last_seen = max_load_key)
    )
select * from tst);