select * from DEV_EDW.STAGING.DST_EMPLOYER 
where cust_no in ('1383145') ;


select  
        row_number() over (partition by customer_number, date_trunc(day,load_datetime) order by load_datetime desc) as seqno,
        * 
from prd_edw.edw_staging_dim.dim_employer
where customer_number in ('1383145') order by load_datetime;


select  
        row_number() over (partition by customer_number, date_trunc(day,load_datetime) order by update_datetime desc) as seqno,
        * 
from prd_edw.edw_staging_dim.dim_employer
where customer_number in ('1383145') 
qualify seqno = 1
order by load_datetime;


select current_timestamp(), current_date::timestamp, current_date::timestamp to_char;


create or replace task RPD1.PUBLIC.TASK_PURGE_AUDIT_HISTORY
    warehouse=WH_EL_PROD
    schedule='USING CRON 0 0 1 1 * America/New_York'
    as delete from bwc_audit.audit_history.Query_History  where start_time::DATE < dateadd(year, -5, current_date);

select * from bwc_audit.audit_history.Query_History  where start_time::DATE < dateadd(year, -5, current_date);



create or replace view DEV_EDW_32600145.PUBLIC.DEV_PRD_CHANGES(
	TABLE_SCHEMA,
	TABLE_NAME,
	COLUMN_NAME,
	PRD_EDW,
	DEV_EDW,
	TYPE
) as (
with 
src as (select 'D' as IND, table_schema, table_name, column_name, data_type, 
            IFF(NUMERIC_PRECISION is not null,NUMERIC_PRECISION||','||NUMERIC_SCALE,'') AS PS
        from DEV_EDW.information_schema.columns where table_catalog = 'DEV_EDW' and table_schema in( 'DIMENSIONS', 'MEDICAL_MART','CLAIMS_MART','POLICY_MART') ),
tgt as (select 'P' as IND, table_schema, table_name, column_name, data_type, 
            IFF(NUMERIC_PRECISION is not null,NUMERIC_PRECISION||','||NUMERIC_SCALE,'') AS PS 
        from PRD_EDW.information_schema.columns where table_catalog = 'PRD_EDW' and table_schema in( 'DIMENSIONS', 'MEDICAL_MART','CLAIMS_MART','POLICY_MART')),
compare as
(select table_schema, table_name, column_name, IND, data_type || iff(ps<>'', '('||PS||')', '') as DT    from src 
UNION ALL 
 select table_schema, table_name, column_name, IND, data_type || iff(ps<>'', '('||PS||')', '') as DT    from tgt),
diff as (
select table_schema, table_name, column_name,
    max(case when IND = 'P' then 'X' else '-' end ) as PRD_EDW,
    max(case when IND = 'D' then 'X' else '-' end ) as DEV_EDW,
    max(case when IND = 'P' then DT else '-' end ) as PRD_TYPE,
    max(case when IND = 'D' then DT else '-' end ) as DEV_TYPE,
    max(case when IND = 'P' then DT || ' -> ' else '+ ' end ) ||
    max(case when IND = 'D' then DT else '-' end ) as TYPE
from compare
group by table_schema, table_name, column_name
having DEV_EDW<>PRD_EDW or PRD_TYPE<>DEV_TYPE)
select table_schema,table_name, column_name, PRD_EDW, DEV_EDW, TYPE from diff
order by 1,2,3
);

desc table dev_edw.dimensions.dim_customer;
desc table prd_edw.dimensions.dim_customer;

use secondary roles all;

select * from DEV_EDW_32600145.PUBLIC.DEV_PRD_CHANGES;
grant select on DEV_EDW_32600145.PUBLIC.DEV_PRD_CHANGES to role DW_ADMIN;
