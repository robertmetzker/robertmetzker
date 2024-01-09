create or replace view DEV_PRD_CHANGES 
(
      TABLE_SCHEMA
    , TABLE_NAME
    , COLUMN_NAME
    , PRD_IND
    , DEV_IND
    , TYPE
) as (
with SRC as (
    select 
        'D' as IND
        , table_schema
        , table_name
        , column_name
        , data_type
        , IFF( numeric_precision is not null, numeric_precision||'.'||numeric_scale, '') as PS
    from  DEV_EDW.information_schema.columns
    where table_catalog = 'DEV_EDW'
      and table_schema in ('DIMENSIONS','MARTS') 
),
TGT as (
    select 
        'P' as IND
        , table_schema
        , table_name
        , column_name
        , data_type
        , IFF( numeric_precision is not null, numeric_precision||'.'||numeric_scale, '') as PS
    from  PRD_EDW.information_schema.columns
    where table_catalog = 'PRD_EDW'
      and table_schema in ('DIMENSIONS','MARTS') 
),
COMPARE as (
    select    table_schema, table_name, column_name, IND
            , data_type || iff( PS <>'', '('||PS||')', '') as DT
        from SRC
    UNION ALL
    select    table_schema, table_name, column_name, IND
            , data_type || iff( PS<>'', '('||PS||')', '') as DT
        from TGT
),        
DIFF as (
    select
          TABLE_SCHEMA
        , TABLE_NAME
        , COLUMN_NAME
        , max( case when IND = 'P' then 'X' else '-' end ) as PRD_EDW
        , max( case when IND = 'D' then 'X' else '-' end ) as DEV_EDW
        , max( case when IND = 'P' then DT  else '-' end ) as PRD_TYPE
        , max( case when IND = 'D' then DT  else '-' end ) as DEV_TYPE
        , max( case when IND = 'P' then DT || ' -> ' else '+ ' end ) ||
          max( case when IND = 'D' then DT else '-' end )  as TYPE
from COMPARE
group by TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
having DEV_EDW <> PRD_EDW or PRD_TYPE <> DEV_TYPE 
)
/* final select comparison */
select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, PRD_EDW, DEV_EDW, TYPE 
  from DIFF
order by 1,2,3
);
