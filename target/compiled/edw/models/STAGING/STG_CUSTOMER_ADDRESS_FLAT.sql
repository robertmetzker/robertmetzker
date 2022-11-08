----SRC LAYER----
with src1 as (
              select distinct
                     cust_id, cust_addr_eff_date
               /* from  STAGING.STG_CUSTOMER_ADDRESS */
               from   STAGING.STG_CUSTOMER_ADDRESS
               where CUST_ADDR_TYP_CD in ('MAIL', 'PHYS' )
                  and VOID_IND = 'N'
            --where cust_id in ('2085799')
),
src as (
              select
                     cust_id, cust_addr_eff_date as start_eff_date,
                     lead(cust_addr_eff_date) over (partition by cust_id order by cust_addr_eff_date) as next_eff_date
               from  src1
),
addr as (
              select distinct
                  cust_id, cust_addr_eff_date,

                  max(case when CUST_ADDR_TYP_CD = 'MAIL' then CUST_ADDR_STR_1   else null end) as MAIL_STR1,
                  max(case when CUST_ADDR_TYP_CD = 'MAIL' then CUST_ADDR_STR_2   else null end) as MAIL_STR2,
                  max(case when CUST_ADDR_TYP_CD = 'MAIL' then CUST_ADDR_CITY_NM else null end) as MAIL_CITY,
                  max(case when CUST_ADDR_TYP_CD = 'MAIL' then STT_ABRV          else null end) as MAIL_ST,
                  max(case when CUST_ADDR_TYP_CD = 'MAIL' then CUST_ADDR_CNTY_NM else null end) as MAIL_CNTY_NM,
                  max(case when CUST_ADDR_TYP_CD = 'MAIL' then CUST_ADDR_POST_CD else null end) as MAIL_POST_CD,
--
                  max(case when CUST_ADDR_TYP_CD = 'PHYS' then CUST_ADDR_STR_1   else null end) as PHYS_STR1,
                  max(case when CUST_ADDR_TYP_CD = 'PHYS' then CUST_ADDR_STR_2   else null end) as PHYS_STR2,
                  max(case when CUST_ADDR_TYP_CD = 'PHYS' then CUST_ADDR_CITY_NM else null end) as PHYS_CITY,
                  max(case when CUST_ADDR_TYP_CD = 'PHYS' then STT_ABRV          else null end) as PHYS_ST,
                  max(case when CUST_ADDR_TYP_CD = 'PHYS' then CUST_ADDR_CNTY_NM else null end) as PHYS_CNTY_NM,
                  max(case when CUST_ADDR_TYP_CD = 'PHYS' then CUST_ADDR_POST_CD else null end) as PHYS_POST_CD
--
               /* from  STAGING.STG_CUSTOMER_ADDRESS */
               from   STAGING.STG_CUSTOMER_ADDRESS
              --where cust_id in ('2085799' )
            where 
                  CUST_ADDR_TYP_CD in ('MAIL', 'PHYS' )
                  and VOID_IND = 'N'
            group by 1,2
)
select  src.start_eff_date, 
        dateadd(day,-1,src.next_eff_date) as next_eff_date,
        addr.*
from src
left join addr 
    on (src.cust_id = addr.cust_id 
        and addr.cust_addr_eff_date between src.start_eff_date and dateadd(day,-1, nvl(src.next_eff_date, current_date)) )
order by src.start_eff_date