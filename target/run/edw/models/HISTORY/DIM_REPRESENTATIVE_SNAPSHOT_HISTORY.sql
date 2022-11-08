

      create or replace  table DEV_EDW.HISTORY.DIM_REPRESENTATIVE_SNAPSHOT_HISTORY  as
      (---------PHASE 2 create table
--CREATE OR REPLACE table dev_edw.history.dim_representative_step1 as 
with 


--********************************************************************************
-----------------------------setting up generic table tbl0_setup
---- rename to generic col names and select cols for history
tbl0_setup1 as (
select distinct 
                             md5( coalesce(cast( CUST_ID as varchar ), '' ) ) as UNIQUE_ID_KEY,
    --data cols
              
                             CUST_NM_NM as STG_CUSTOMER_NAME_CUST_NM_NM
,

    --start end cols
    
        IFF( IFNULL( try_to_timestamp( CUST_NM_EFF_DT::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), CUST_NM_EFF_DT )
          as dbt_valid_from_initial_1,
    
        IFF( IFNULL( try_to_timestamp( CUST_NM_END_DT::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), CUST_NM_END_DT )
          as dbt_valid_to_1 

    from STG_CUSTOMER_NAME
    WHERE CUST_NM_TYP_CD IN ('PRSN_NM', 'BUSN_LEGAL_NM') AND VOID_IND = 'N'
        )
---- only select latest values for a day
, tbl0_setup2 as (
    select *, 
           ROW_NUMBER () OVER ( PARTITION BY UNIQUE_ID_KEY, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) ORDER BY DBT_VALID_FROM_INITIAL_1 DESC ) AS tbl0_ROWN 
    from     tbl0_setup1 
            qualify tbl0_ROWN = 1
        )
---- convert to a date, for comparisons across tables time differences do not matter, need to convert to ts at the end for dbt
, tbl0_setup as (
    select *, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) as DBT_VALID_FROM_INITIAL, TO_DATE( dbt_valid_to_1 ) as dbt_valid_to
    from     tbl0_setup2 
        )        
--  select * from tbl0_setup;       
--********************************************************************************----------------------------- creating collapse id of all tracked columns and making it unique to remove duplicate rows
    , tbl0_setup_build_granularity as (
        select *, min( UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_NAME_CUST_NM_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' ) ) OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_NAME_CUST_NM_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )) as tbl0_setup_collapse_id
    from tbl0_setup
    )



    , tbl0_setup_add_lag as ( --- first collapse
    SELECT
        *,
        ROW_NUMBER() OVER( PARTITION BY tbl0_setup_collapse_id ORDER BY dbt_valid_from_initial, dbt_valid_to) AS RN,
        LAG( dbt_valid_to, 1 ) OVER ( PARTITION BY tbl0_setup_collapse_id ORDER BY dbt_valid_from_initial, dbt_valid_to) AS PEND
    FROM
         tbl0_setup_build_granularity
    )
    --- mark all rows that have duplicate values for a range of dates based on the row number that starts the dates
--                                GAPID(starting row number)
--                                    SUM
-- Jan1,Jan 2 a  ->  Jan1,Jan2 a   23 23
-- Jan2,Jan 3 a  ->  Jan2,Jan3 a   0  23
-- Jan3,Jan 4 a  ->  Jan3,Jan4 a   0  23

    , tbl0_setup_add_gapid as (
    select 
            *,
            case when dbt_valid_from_initial > dateadd( day, 1, tbl0_setup_add_lag.PEND ) then RN else 0 END as gap,
            SUM( gap ) OVER ( PARTITION BY tbl0_setup_collapse_id ORDER BY tbl0_setup_add_lag.RN ) AS gapid

--- group on the gapid to remove duplicate rows and reset start and end dates in a criss-cross(upper left, to bottom right)
--- result above becomes: Jan1,Jan 4 a  


    from tbl0_setup_add_lag )
    --- collapse every row with same gapid into one row 
    , tbl0_setup_collapse as 

        ( 
        select 
        tbl0_setup_collapse_id, min( dbt_valid_from_initial ) as dbt_valid_from_initial, max( dbt_valid_to ) as dbt_valid_to
        from tbl0_setup_add_gapid
        group by tbl0_setup_collapse_id, gapid
         )
---- group by removes the columns, use split_part to get the values and columns back               
    , tbl0_setup_granularity as (
        select     nullif(split_part( tbl0_setup_collapse_id, '~',1 ),'THISISANULLVALplaceholdertoberemoved' ) as UNIQUE_ID_KEY, nullif(split_part( tbl0_setup_collapse_id, '~', 2), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_NAME_CUST_NM_NM,  *
        from tbl0_setup_collapse
    ) --elect * from tbl0_setup_granularity limit 10;
    
--    select * from tbl0_setup_granularity;


----------------------filling in any gaps in the dates
--  jan2   jan3      becomes jan2  jan3
--  jan6   jan7              jan3  jan7


, tbl0_add_lag2 as (
SELECT
    *,
    LAG( dbt_valid_to, 1 ) OVER( PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial, dbt_valid_to ) AS PREV_dbt_valid_to,
    ROW_NUMBER() OVER( PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial, dbt_valid_to ) AS RN --  for smushing
    FROM
        tbl0_setup_granularity
order by UNIQUE_ID_KEY, dbt_valid_from_initial
)
--- if jan6( next row initial ) > jan3( previous_to )
, tbl0_mark_gap2 as ( 
select 
        *,
--        case when  dbt_valid_from_initial > DATEADD( 'DAY', 1, PREV_dbt_valid_to )  then 1 else 0 END as gap,
--        case when  dbt_valid_from_initial =  PREV_dbt_valid_to  then DATEADD( 'DAY', 1, dbt_valid_from_initial ) else dbt_valid_from_initial END as dbt_valid_from_2a -- making 
        case when  dbt_valid_from_initial >  PREV_dbt_valid_to   then 1 else 0 END as gap,
        case when  dbt_valid_from_initial =  PREV_dbt_valid_to  then dbt_valid_from_initial  else dbt_valid_from_initial END as dbt_valid_from_2a -- placeholder for dateadd 
from tbl0_add_lag2

)
--- if there is a gap use the previous valid_to as the new valid_from
, tbl0_fill_gap as (
select *,
    --case when gap = 1 then  DATEADD( 'DAY', 1, PREV_dbt_valid_to ) else  dbt_valid_from_2a  end as dbt_valid_from_2b  -- made up history!!!
    case when gap = 1 then  PREV_dbt_valid_to else  dbt_valid_from_2a  end as dbt_valid_from_2b  -- made up history!!!
    from tbl0_mark_gap2
)
--- rewrite dbt_valid_from_2B to be the actual dbt_valid_from
, tbl0 as (
--    select  UNIQUE_ID_KEY, dbt_valid_from_2B as  dbt_valid_from, dbt_valid_to from  fill_gap
select *, dbt_valid_from_2B as  dbt_valid_from from  tbl0_fill_gap
)

-- select * from tbl0;
    ,
--********************************************************************************
-----------------------------setting up generic table tbl1_setup
---- rename to generic col names and select cols for history
tbl1_setup1 as (
select distinct 
                             md5( coalesce(cast( CUST_ID as varchar ), '' ) ) as UNIQUE_ID_KEY,
    --data cols
              
                             DOCUMENT_BLK_IND as STG_CUSTOMER_BLOCK_DOCUMENT_BLOCK_IND
,

    --start end cols
    
        IFF( IFNULL( try_to_timestamp( BLK_EFF_DT::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), BLK_EFF_DT )
          as dbt_valid_from_initial_1,
    
        IFF( IFNULL( try_to_timestamp( BLK_END_DT::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), BLK_END_DT )
          as dbt_valid_to_1 

    from STG_CUSTOMER_BLOCK
   where DRVD_BLK_TYP_CD ='D'
        )
---- only select latest values for a day
, tbl1_setup2 as (
    select *, 
           ROW_NUMBER () OVER ( PARTITION BY UNIQUE_ID_KEY, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) ORDER BY DBT_VALID_FROM_INITIAL_1 DESC ) AS tbl1_ROWN 
    from     tbl1_setup1 
            qualify tbl1_ROWN = 1
        )
---- convert to a date, for comparisons across tables time differences do not matter, need to convert to ts at the end for dbt
, tbl1_setup as (
    select *, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) as DBT_VALID_FROM_INITIAL, TO_DATE( dbt_valid_to_1 ) as dbt_valid_to
    from     tbl1_setup2 
        )        
        

--********************************************************************************----------------------------- creating collapse id of all tracked columns and making it unique to remove duplicate rows
    , tbl1_setup_build_granularity as (
        select *, min( UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_BLOCK_DOCUMENT_BLOCK_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' ) ) OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_BLOCK_DOCUMENT_BLOCK_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' )) as tbl1_setup_collapse_id
    from tbl1_setup
    )



    , tbl1_setup_add_lag as ( --- first collapse
    SELECT
        *,
        ROW_NUMBER() OVER( PARTITION BY tbl1_setup_collapse_id ORDER BY dbt_valid_from_initial, dbt_valid_to) AS RN,
        LAG( dbt_valid_to, 1 ) OVER ( PARTITION BY tbl1_setup_collapse_id ORDER BY dbt_valid_from_initial, dbt_valid_to) AS PEND
    FROM
         tbl1_setup_build_granularity
    )
    --- mark all rows that have duplicate values for a range of dates based on the row number that starts the dates
--                                GAPID(starting row number)
--                                    SUM
-- Jan1,Jan 2 a  ->  Jan1,Jan2 a   23 23
-- Jan2,Jan 3 a  ->  Jan2,Jan3 a   0  23
-- Jan3,Jan 4 a  ->  Jan3,Jan4 a   0  23

    , tbl1_setup_add_gapid as (
    select 
            *,
            case when dbt_valid_from_initial > dateadd( day, 1, tbl1_setup_add_lag.PEND ) then RN else 0 END as gap,
            SUM( gap ) OVER ( PARTITION BY tbl1_setup_collapse_id ORDER BY tbl1_setup_add_lag.RN ) AS gapid

--- group on the gapid to remove duplicate rows and reset start and end dates in a criss-cross(upper left, to bottom right)
--- result above becomes: Jan1,Jan 4 a  


    from tbl1_setup_add_lag )
    --- collapse every row with same gapid into one row 
    , tbl1_setup_collapse as 

        ( 
        select 
        tbl1_setup_collapse_id, min( dbt_valid_from_initial ) as dbt_valid_from_initial, max( dbt_valid_to ) as dbt_valid_to
        from tbl1_setup_add_gapid
        group by tbl1_setup_collapse_id, gapid
         )
---- group by removes the columns, use split_part to get the values and columns back               
    , tbl1_setup_granularity as (
        select     nullif(split_part( tbl1_setup_collapse_id, '~',1 ),'THISISANULLVALplaceholdertoberemoved' ) as UNIQUE_ID_KEY, nullif(split_part( tbl1_setup_collapse_id, '~', 2), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_BLOCK_DOCUMENT_BLOCK_IND,  *
        from tbl1_setup_collapse
    )



----------------------filling in any gaps in the dates
--  jan2   jan3      becomes jan2  jan3
--  jan6   jan7              jan3  jan7


, tbl1_add_lag2 as (
SELECT
    *,
    LAG( dbt_valid_to, 1 ) OVER( PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial, dbt_valid_to ) AS PREV_dbt_valid_to,
    ROW_NUMBER() OVER( PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial, dbt_valid_to ) AS RN --  for smushing
    FROM
        tbl1_setup_granularity
order by UNIQUE_ID_KEY, dbt_valid_from_initial
)
--- if jan6( next row initial ) > jan3( previous_to )
, tbl1_mark_gap2 as ( 
select 
        *,
--        case when  dbt_valid_from_initial > DATEADD( 'DAY', 1, PREV_dbt_valid_to )  then 1 else 0 END as gap,
--        case when  dbt_valid_from_initial =  PREV_dbt_valid_to  then DATEADD( 'DAY', 1, dbt_valid_from_initial ) else dbt_valid_from_initial END as dbt_valid_from_2a -- making 
        case when  dbt_valid_from_initial >  PREV_dbt_valid_to   then 1 else 0 END as gap,
        case when  dbt_valid_from_initial =  PREV_dbt_valid_to  then dbt_valid_from_initial  else dbt_valid_from_initial END as dbt_valid_from_2a -- placeholder for dateadd 
from tbl1_add_lag2

)
--- if there is a gap use the previous valid_to as the new valid_from
, tbl1_fill_gap as (
select *,
    --case when gap = 1 then  DATEADD( 'DAY', 1, PREV_dbt_valid_to ) else  dbt_valid_from_2a  end as dbt_valid_from_2b  -- made up history!!!
    case when gap = 1 then  PREV_dbt_valid_to else  dbt_valid_from_2a  end as dbt_valid_from_2b  -- made up history!!!
    from tbl1_mark_gap2
)
--- rewrite dbt_valid_from_2B to be the actual dbt_valid_from
, tbl1 as (
--    select  UNIQUE_ID_KEY, dbt_valid_from_2B as  dbt_valid_from, dbt_valid_to from  fill_gap
select *, dbt_valid_from_2B as  dbt_valid_from from  tbl1_fill_gap
),

--//select * from tbl1;

--********************************************************************************
-----------------------------setting up generic table tbl2_setup
---- rename to generic col names and select cols for history
tbl2_setup1 as (
select distinct 
                             md5( coalesce(cast( CUST_ID as varchar ), '' ) ) as UNIQUE_ID_KEY,
    --data cols
              
                             THREATENING_BEHAVIOR_BLOCK_IND as STG_CUSTOMER_BLOCK_THREATENING_BEHAVIOR_BLOCK_IND
,

    --start end cols
    
        IFF( IFNULL( try_to_timestamp( BLK_EFF_DT::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), BLK_EFF_DT )
          as dbt_valid_from_initial_1,
    
        IFF( IFNULL( try_to_timestamp( BLK_END_DT::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), BLK_END_DT )
          as dbt_valid_to_1 

    from STG_CUSTOMER_BLOCK
    WHERE DRVD_BLK_TYP_CD = 'A'
      --WHERE BLK_TYP_CD = 'ALERT' AND VOID_IND = 'N' AND DRVD_BLK_TYP_CD = 'A'
        )
---- only select latest values for a day
, tbl2_setup2 as (
    select *, 
           ROW_NUMBER () OVER ( PARTITION BY UNIQUE_ID_KEY, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) ORDER BY DBT_VALID_FROM_INITIAL_1 DESC ) AS tbl2_ROWN 
    from     tbl2_setup1 
            qualify tbl2_ROWN = 1
        )
---- convert to a date, for comparisons across tables time differences do not matter, need to convert to ts at the end for dbt
, tbl2_setup as (
    select *, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) as DBT_VALID_FROM_INITIAL, TO_DATE( dbt_valid_to_1 ) as dbt_valid_to
    from     tbl2_setup2 
        )        
        

--********************************************************************************----------------------------- creating collapse id of all tracked columns and making it unique to remove duplicate rows
    , tbl2_setup_build_granularity as (
        select *, min( UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_BLOCK_THREATENING_BEHAVIOR_BLOCK_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' ) ) OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_BLOCK_THREATENING_BEHAVIOR_BLOCK_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' )) as tbl2_setup_collapse_id
    from tbl2_setup
    )



    , tbl2_setup_add_lag as ( --- first collapse
    SELECT
        *,
        ROW_NUMBER() OVER( PARTITION BY tbl2_setup_collapse_id ORDER BY dbt_valid_from_initial, dbt_valid_to) AS RN,
        LAG( dbt_valid_to, 1 ) OVER ( PARTITION BY tbl2_setup_collapse_id ORDER BY dbt_valid_from_initial, dbt_valid_to) AS PEND
    FROM
         tbl2_setup_build_granularity
    )
   --- mark all rows that have duplicate values for a range of dates based on the row number that starts the dates
--                                GAPID(starting row number)
--                                    SUM
-- Jan1,Jan 2 a  ->  Jan1,Jan2 a   23 23
-- Jan2,Jan 3 a  ->  Jan2,Jan3 a   0  23
-- Jan3,Jan 4 a  ->  Jan3,Jan4 a   0  23

    , tbl2_setup_add_gapid as (
    select 
            *,
            case when dbt_valid_from_initial > dateadd( day, 1, tbl2_setup_add_lag.PEND ) then RN else 0 END as gap,
            SUM( gap ) OVER ( PARTITION BY tbl2_setup_collapse_id ORDER BY tbl2_setup_add_lag.RN ) AS gapid

--- group on the gapid to remove duplicate rows and reset start and end dates in a criss-cross(upper left, to bottom right)
--- result above becomes: Jan1,Jan 4 a  


    from tbl2_setup_add_lag )
    --- collapse every row with same gapid into one row 
    , tbl2_setup_collapse as 

       ( 
        select 
        tbl2_setup_collapse_id, min( dbt_valid_from_initial ) as dbt_valid_from_initial, max( dbt_valid_to ) as dbt_valid_to
        from tbl2_setup_add_gapid
        group by tbl2_setup_collapse_id, gapid
         )
---- group by removes the columns, use split_part to get the values and columns back               
    , tbl2_setup_granularity as (
        select     nullif(split_part( tbl2_setup_collapse_id, '~',1 ),'THISISANULLVALplaceholdertoberemoved' ) as UNIQUE_ID_KEY, nullif(split_part( tbl2_setup_collapse_id, '~', 2), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_BLOCK_THREATENING_BEHAVIOR_BLOCK_IND,  *
        from tbl2_setup_collapse
    )



----------------------filling in any gaps in the dates
--  jan2   jan3      becomes jan2  jan3
--  jan6   jan7              jan3  jan7


, tbl2_add_lag2 as (
SELECT
    *,
    LAG( dbt_valid_to, 1 ) OVER( PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial, dbt_valid_to ) AS PREV_dbt_valid_to,
    ROW_NUMBER() OVER( PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial, dbt_valid_to ) AS RN --  for smushing
    FROM
        tbl2_setup_granularity
order by UNIQUE_ID_KEY, dbt_valid_from_initial
)
--- if jan6( next row initial ) > jan3( previous_to )
, tbl2_mark_gap2 as ( 
select 
        *,
--        case when  dbt_valid_from_initial > DATEADD( 'DAY', 1, PREV_dbt_valid_to )  then 1 else 0 END as gap,
--        case when  dbt_valid_from_initial =  PREV_dbt_valid_to  then DATEADD( 'DAY', 1, dbt_valid_from_initial ) else dbt_valid_from_initial END as dbt_valid_from_2a -- making 
        case when  dbt_valid_from_initial >  PREV_dbt_valid_to   then 1 else 0 END as gap,
        case when  dbt_valid_from_initial =  PREV_dbt_valid_to  then dbt_valid_from_initial  else dbt_valid_from_initial END as dbt_valid_from_2a -- placeholder for dateadd 
from tbl2_add_lag2

)
--- if there is a gap use the previous valid_to as the new valid_from
, tbl2_fill_gap as (
select *,
    --case when gap = 1 then  DATEADD( 'DAY', 1, PREV_dbt_valid_to ) else  dbt_valid_from_2a  end as dbt_valid_from_2b  -- made up history!!!
    case when gap = 1 then  PREV_dbt_valid_to else  dbt_valid_from_2a  end as dbt_valid_from_2b  -- made up history!!!
    from tbl2_mark_gap2
)
--- rewrite dbt_valid_from_2B to be the actual dbt_valid_from
, tbl2 as (
--    select  UNIQUE_ID_KEY, dbt_valid_from_2B as  dbt_valid_from, dbt_valid_to from  fill_gap
select *, dbt_valid_from_2B as  dbt_valid_from from  tbl2_fill_gap
)

    ,
--********************************************************************************
-----------------------------setting up generic table tbl3_setup
---- rename to generic col names and select cols for history
tbl3_setup1 as (
select distinct 
                             md5( coalesce(cast( CUST_ID as varchar ), '' ) ) as UNIQUE_ID_KEY,
    --data cols
              
                             CNTRY_NM as STG_CUSTOMER_ADDRESS_CNTRY_NM, 
                             CUST_ADDR_CITY_NM as STG_CUSTOMER_ADDRESS_CUST_ADDR_CITY_NM, 
                             CUST_ADDR_CNTY_NM as STG_CUSTOMER_ADDRESS_CUST_ADDR_CNTY_NM, 
                             CUST_ADDR_COMT as STG_CUSTOMER_ADDRESS_CUST_ADDR_COMT, 
                             CUST_ADDR_POST_CD as STG_CUSTOMER_ADDRESS_CUST_ADDR_POST_CD, 
                             CUST_ADDR_STR_1 as STG_CUSTOMER_ADDRESS_CUST_ADDR_STR_1, 
                             CUST_ADDR_STR_2 as STG_CUSTOMER_ADDRESS_CUST_ADDR_STR_2, 
                             CUST_ADDR_VLDT_IND as STG_CUSTOMER_ADDRESS_CUST_ADDR_VLDT_IND, 
                             STT_ABRV as STG_CUSTOMER_ADDRESS_STT_ABRV, 
                             STT_NM as STG_CUSTOMER_ADDRESS_STT_NM,
                             CUST_ADDR_EFF_DATE as MAILING_ADDRESS_EFFECTIVE_DATE,
                             CUST_ADDR_END_DATE as MAILING_ADDRESS_END_DATE
,

    --start end cols
    
        IFF( IFNULL( try_to_timestamp( CUST_ADDR_EFF_DATE::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), CUST_ADDR_EFF_DATE )
          as dbt_valid_from_initial_1,
    
        IFF( IFNULL( try_to_timestamp( DRVD_CUST_ADDR_END_DATE::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), DRVD_CUST_ADDR_END_DATE )
         as dbt_valid_to_1 

    from STG_CUSTOMER_ADDRESS
    WHERE CUST_ADDR_TYP_CD = 'MAIL' AND VOID_IND = 'N' 
        )
      ---- only select latest values for a day
, tbl3_setup2 as (
    select *, 
           ROW_NUMBER () OVER ( PARTITION BY UNIQUE_ID_KEY, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) ORDER BY DBT_VALID_FROM_INITIAL_1 DESC ) AS tbl3_ROWN 
    from     tbl3_setup1 
            qualify tbl3_ROWN = 1
        )
---- convert to a date, for comparisons across tables time differences do not matter, need to convert to ts at the end for dbt
, tbl3_setup as (
    select *, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) as DBT_VALID_FROM_INITIAL, TO_DATE( dbt_valid_to_1 ) as dbt_valid_to
    from     tbl3_setup2 
        )        
        

--********************************************************************************----------------------------- creating collapse id of all tracked columns and making it unique to remove duplicate rows
,tbl3_setup_build_granularity as (
	  select *, min( UNIQUE_ID_KEY|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_CUST_ADDR_STR_1 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_CUST_ADDR_STR_2 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_CUST_ADDR_CITY_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_STT_ABRV as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_STT_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_CUST_ADDR_POST_CD as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_CUST_ADDR_CNTY_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_CNTRY_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_CUST_ADDR_VLDT_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~'
		 ||coalesce(cast( STG_CUSTOMER_ADDRESS_CUST_ADDR_COMT as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~'
         ||coalesce(cast( MAILING_ADDRESS_EFFECTIVE_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~'		 
         ||coalesce(cast( MAILING_ADDRESS_END_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved' ) ) 
         OVER( PARTITION BY UNIQUE_ID_KEY|| '~' 
		 ||coalesce(cast( STG_CUSTOMER_ADDRESS_CUST_ADDR_STR_1 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_CUST_ADDR_STR_2 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_CUST_ADDR_CITY_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_STT_ABRV as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_STT_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_CUST_ADDR_POST_CD as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_CUST_ADDR_CNTY_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_CNTRY_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
         ||coalesce(cast( STG_CUSTOMER_ADDRESS_CUST_ADDR_VLDT_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~'
	     ||coalesce(cast( STG_CUSTOMER_ADDRESS_CUST_ADDR_COMT as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~'
         ||coalesce(cast( MAILING_ADDRESS_EFFECTIVE_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~'		 
         ||coalesce(cast( MAILING_ADDRESS_END_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )) as tbl3_setup_collapse_id
    from tbl3_setup
)


    , tbl3_setup_add_lag as ( --- first collapse
    SELECT
        *,
        ROW_NUMBER() OVER( PARTITION BY tbl3_setup_collapse_id ORDER BY dbt_valid_from_initial, dbt_valid_to) AS RN,
        LAG( dbt_valid_to, 1 ) OVER ( PARTITION BY tbl3_setup_collapse_id ORDER BY dbt_valid_from_initial, dbt_valid_to) AS PEND
    FROM
         tbl3_setup_build_granularity
    )
    
    
    --- mark all rows that have duplicate values for a range of dates based on the row number that starts the dates
--                                GAPID(starting row number)
--                                    SUM
-- Jan1,Jan 2 a  ->  Jan1,Jan2 a   23 23
-- Jan2,Jan 3 a  ->  Jan2,Jan3 a   0  23
-- Jan3,Jan 4 a  ->  Jan3,Jan4 a   0  23

    , tbl3_setup_add_gapid as (
    select 
            *,
            case when dbt_valid_from_initial > dateadd( day, 1, tbl3_setup_add_lag.PEND ) then RN else 0 END as gap,
            SUM( gap ) OVER ( PARTITION BY tbl3_setup_collapse_id ORDER BY tbl3_setup_add_lag.RN ) AS gapid

--- group on the gapid to remove duplicate rows and reset start and end dates in a criss-cross(upper left, to bottom right)
--- result above becomes: Jan1,Jan 4 a  


    from tbl3_setup_add_lag )
    --- collapse every row with same gapid into one row 
    , tbl3_setup_collapse as 

        ( 
        select 
        tbl3_setup_collapse_id,gapid, min( dbt_valid_from_initial ) as dbt_valid_from_initial, max( dbt_valid_to ) as dbt_valid_to
        from tbl3_setup_add_gapid
        group by tbl3_setup_collapse_id, gapid
         )
         
         ---- group by removes the columns, use split_part to get the values and columns back               
    , tbl3_setup_granularity as (
        select     
		nullif(split_part( tbl3_setup_collapse_id, '~',1 ),'THISISANULLVALplaceholdertoberemoved' ) as UNIQUE_ID_KEY, 
		nullif(split_part( tbl3_setup_collapse_id, '~', 2), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_CUST_ADDR_STR_1, 
		nullif(split_part( tbl3_setup_collapse_id, '~', 3), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_CUST_ADDR_STR_2, 
		nullif(split_part( tbl3_setup_collapse_id, '~', 4), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_CUST_ADDR_CITY_NM, 
		nullif(split_part( tbl3_setup_collapse_id, '~', 5), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_STT_ABRV, 
		nullif(split_part( tbl3_setup_collapse_id, '~', 6), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_STT_NM, 
		nullif(split_part( tbl3_setup_collapse_id, '~', 7), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_CUST_ADDR_POST_CD, 
		nullif(split_part( tbl3_setup_collapse_id, '~', 8), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_CUST_ADDR_CNTY_NM,
        nullif(split_part( tbl3_setup_collapse_id, '~', 9), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_CNTRY_NM, 
	    nullif(split_part( tbl3_setup_collapse_id, '~', 10), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_CUST_ADDR_VLDT_IND, 
	    nullif(split_part( tbl3_setup_collapse_id, '~', 11), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_CUST_ADDR_COMT, 
        nullif(split_part( tbl3_setup_collapse_id, '~', 12), 'THISISANULLVALplaceholdertoberemoved' ) as MAILING_ADDRESS_EFFECTIVE_DATE,
		nullif(split_part( tbl3_setup_collapse_id, '~', 13), 'THISISANULLVALplaceholdertoberemoved' ) as MAILING_ADDRESS_END_DATE,
		*
        from tbl3_setup_collapse
    )

----------------------filling in any gaps in the dates
--  jan2   jan3      becomes jan2  jan3
--  jan6   jan7              jan3  jan7


, tbl3_add_lag2 as (
SELECT
    *,
    LAG( dbt_valid_to, 1 ) OVER( PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial, dbt_valid_to ) AS PREV_dbt_valid_to,
    ROW_NUMBER() OVER( PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial, dbt_valid_to ) AS RN --  for smushing
    FROM
        tbl3_setup_granularity
order by UNIQUE_ID_KEY, dbt_valid_from_initial
)
--- if jan6( next row initial ) > jan3( previous_to )
, tbl3_mark_gap2 as ( 
select 
        *,
--        case when  dbt_valid_from_initial > DATEADD( 'DAY', 1, PREV_dbt_valid_to )  then 1 else 0 END as gap,
--        case when  dbt_valid_from_initial =  PREV_dbt_valid_to  then DATEADD( 'DAY', 1, dbt_valid_from_initial ) else dbt_valid_from_initial END as dbt_valid_from_2a -- making 
        case when  dbt_valid_from_initial >  PREV_dbt_valid_to   then 1 else 0 END as gap,
        case when  dbt_valid_from_initial =  PREV_dbt_valid_to  then dbt_valid_from_initial  else dbt_valid_from_initial END as dbt_valid_from_2a -- placeholder for dateadd 
from tbl3_add_lag2

)
--- if there is a gap use the previous valid_to as the new valid_from
, tbl3_fill_gap as (
select *,
    --case when gap = 1 then  DATEADD( 'DAY', 1, PREV_dbt_valid_to ) else  dbt_valid_from_2a  end as dbt_valid_from_2b  -- made up history!!!
    case when gap = 1 then  PREV_dbt_valid_to else  dbt_valid_from_2a  end as dbt_valid_from_2b  -- made up history!!!
    from tbl3_mark_gap2
)
--- rewrite dbt_valid_from_2B to be the actual dbt_valid_from
, tbl3 as (
--    select  UNIQUE_ID_KEY, dbt_valid_from_2B as  dbt_valid_from, dbt_valid_to from  fill_gap
select *, dbt_valid_from_2B as  dbt_valid_from from  tbl3_fill_gap
)

    ,
--********************************************************************************
-----------------------------setting up generic table tbl4_setup
---- rename to generic col names and select cols for history
tbl4_setup1 as (
select distinct 
                             md5( coalesce(cast( CUST_ID as varchar ), '' ) ) as UNIQUE_ID_KEY,
    --data cols
              
                             CNTRY_NM as STG_CUSTOMER_ADDRESS_PHYS_CNTRY_NM, 
                             CUST_ADDR_CITY_NM as STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_CITY_NM, 
                             CUST_ADDR_CNTY_NM as STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_CNTY_NM, 
                             CUST_ADDR_COMT as STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_COMT, 
                             CUST_ADDR_POST_CD as STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_POST_CD, 
                             CUST_ADDR_STR_1 as STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_STR_1, 
                             CUST_ADDR_STR_2 as STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_STR_2, 
                             CUST_ADDR_VLDT_IND as STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_VLDT_IND, 
                             STT_ABRV as STG_CUSTOMER_ADDRESS_PHYS_STT_ABRV, 
                             STT_NM as STG_CUSTOMER_ADDRESS_PHYS_STT_NM,
                             CUST_ADDR_EFF_DATE as PHYSICAL_ADDRESS_EFFECTIVE_DATE,
                             CUST_ADDR_END_DATE as PHYSICAL_ADDRESS_END_DATE
,

    --start end cols
    
        IFF( IFNULL( try_to_timestamp( CUST_ADDR_EFF_DATE::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), CUST_ADDR_EFF_DATE )
          as dbt_valid_from_initial_1,
    
        IFF( IFNULL( try_to_timestamp( DRVD_CUST_ADDR_END_DATE::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), DRVD_CUST_ADDR_END_DATE )
          as dbt_valid_to_1 

    from STG_CUSTOMER_ADDRESS
    WHERE CUST_ADDR_TYP_CD = 'PHYS' AND VOID_IND = 'N'
        )
---- only select latest values for a day
, tbl4_setup2 as (
    select *, 
           ROW_NUMBER () OVER ( PARTITION BY UNIQUE_ID_KEY, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) ORDER BY DBT_VALID_FROM_INITIAL_1 DESC ) AS tbl4_ROWN 
    from     tbl4_setup1 
            qualify tbl4_ROWN = 1
        )
---- convert to a date, for comparisons across tables time differences do not matter, need to convert to ts at the end for dbt
, tbl4_setup as (
    select *, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) as DBT_VALID_FROM_INITIAL, TO_DATE( dbt_valid_to_1 ) as dbt_valid_to
    from     tbl4_setup2 
        )      
        

--********************************************************************************----------------------------- creating collapse id of all tracked columns and making it unique to remove duplicate rows
    , tbl4_setup_build_granularity as (
        select *, min( UNIQUE_ID_KEY|| '~' 
                      ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_STR_1 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
                      ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_STR_2 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
                      ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_CITY_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
                      ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_STT_ABRV as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
                      ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_STT_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
                      ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_POST_CD as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
                      ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_CNTY_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
                      ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CNTRY_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
                      ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_VLDT_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~'
                      ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_COMT as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~'
                      ||coalesce(cast( PHYSICAL_ADDRESS_EFFECTIVE_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~'
                      ||coalesce(cast( PHYSICAL_ADDRESS_END_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved' ) ) 
      OVER(PARTITION BY UNIQUE_ID_KEY|| '~' 
           ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_STR_1 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
           ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_STR_2 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
           ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_CITY_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
           ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_STT_ABRV as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
           ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_STT_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
           ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_POST_CD as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
           ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_CNTY_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
           ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CNTRY_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' 
           ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_VLDT_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~'
           ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_COMT as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~'
           ||coalesce(cast( PHYSICAL_ADDRESS_EFFECTIVE_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~'
           ||coalesce(cast( PHYSICAL_ADDRESS_END_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )) as tbl4_setup_collapse_id
    from tbl4_setup
    )


    , tbl4_setup_add_lag as ( --- first collapse
    SELECT
        *,
        ROW_NUMBER() OVER( PARTITION BY tbl4_setup_collapse_id ORDER BY dbt_valid_from_initial, dbt_valid_to) AS RN,
        LAG( dbt_valid_to, 1 ) OVER ( PARTITION BY tbl4_setup_collapse_id ORDER BY dbt_valid_from_initial, dbt_valid_to) AS PEND
    FROM
       tbl4_setup_build_granularity
    )
    --- mark all rows that have duplicate values for a range of dates based on the row number that starts the dates
--                                GAPID(starting row number)
--                                    SUM
-- Jan1,Jan 2 a  ->  Jan1,Jan2 a   23 23
-- Jan2,Jan 3 a  ->  Jan2,Jan3 a   0  23
-- Jan3,Jan 4 a  ->  Jan3,Jan4 a   0  23

    , tbl4_setup_add_gapid as (
    select 
            *,
            case when dbt_valid_from_initial > dateadd( day, 1, tbl4_setup_add_lag.PEND ) then RN else 0 END as gap,
            SUM( gap ) OVER ( PARTITION BY tbl4_setup_collapse_id ORDER BY tbl4_setup_add_lag.RN ) AS gapid

--- group on the gapid to remove duplicate rows and reset start and end dates in a criss-cross(upper left, to bottom right)
--- result above becomes: Jan1,Jan 4 a  


    from tbl4_setup_add_lag )
    --- collapse every row with same gapid into one row 
    , tbl4_setup_collapse as 

        ( 
        select 
        tbl4_setup_collapse_id, min( dbt_valid_from_initial ) as dbt_valid_from_initial, max( dbt_valid_to ) as dbt_valid_to
        from tbl4_setup_add_gapid
        group by tbl4_setup_collapse_id, gapid
         )
---- group by removes the columns, use split_part to get the values and columns back               
    , tbl4_setup_granularity as (
        select     
      nullif(split_part( tbl4_setup_collapse_id, '~',1 ),'THISISANULLVALplaceholdertoberemoved' ) as UNIQUE_ID_KEY, 
      nullif(split_part( tbl4_setup_collapse_id, '~', 2), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_STR_1, 
      nullif(split_part( tbl4_setup_collapse_id, '~', 3), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_STR_2, 
      nullif(split_part( tbl4_setup_collapse_id, '~', 4), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_CITY_NM, 
      nullif(split_part( tbl4_setup_collapse_id, '~', 5), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_STT_ABRV, 
      nullif(split_part( tbl4_setup_collapse_id, '~', 6), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_STT_NM,
      nullif(split_part( tbl4_setup_collapse_id, '~', 7), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_POST_CD, 
      nullif(split_part( tbl4_setup_collapse_id, '~', 8), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_CNTY_NM, 
      nullif(split_part( tbl4_setup_collapse_id, '~', 9), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_CNTRY_NM, 
      nullif(split_part( tbl4_setup_collapse_id, '~', 10), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_VLDT_IND, 
      nullif(split_part( tbl4_setup_collapse_id, '~', 11), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_COMT,
      nullif(split_part( tbl4_setup_collapse_id, '~', 12), 'THISISANULLVALplaceholdertoberemoved' ) as PHYSICAL_ADDRESS_EFFECTIVE_DATE,
      nullif(split_part( tbl4_setup_collapse_id, '~', 13), 'THISISANULLVALplaceholdertoberemoved' ) as PHYSICAL_ADDRESS_END_DATE,*
        from tbl4_setup_collapse
    )


----------------------filling in any gaps in the dates
--  jan2   jan3      becomes jan2  jan3
--  jan6   jan7              jan3  jan7


, tbl4_add_lag2 as (
SELECT
    *,
    LAG( dbt_valid_to, 1 ) OVER( PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial, dbt_valid_to ) AS PREV_dbt_valid_to,
    ROW_NUMBER() OVER( PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial, dbt_valid_to ) AS RN --  for smushing
    FROM
        tbl4_setup_granularity
order by UNIQUE_ID_KEY, dbt_valid_from_initial
)
--- if jan6( next row initial ) > jan3( previous_to )
, tbl4_mark_gap2 as ( 
select 
        *,
--        case when  dbt_valid_from_initial > DATEADD( 'DAY', 1, PREV_dbt_valid_to )  then 1 else 0 END as gap,
--        case when  dbt_valid_from_initial =  PREV_dbt_valid_to  then DATEADD( 'DAY', 1, dbt_valid_from_initial ) else dbt_valid_from_initial END as dbt_valid_from_2a -- making 
        case when  dbt_valid_from_initial >  PREV_dbt_valid_to   then 1 else 0 END as gap,
        case when  dbt_valid_from_initial =  PREV_dbt_valid_to  then dbt_valid_from_initial  else dbt_valid_from_initial END as dbt_valid_from_2a -- placeholder for dateadd 
from tbl4_add_lag2

)
--- if there is a gap use the previous valid_to as the new valid_from
, tbl4_fill_gap as (
select *,
    --case when gap = 1 then  DATEADD( 'DAY', 1, PREV_dbt_valid_to ) else  dbt_valid_from_2a  end as dbt_valid_from_2b  -- made up history!!!
    case when gap = 1 then  PREV_dbt_valid_to else  dbt_valid_from_2a  end as dbt_valid_from_2b  -- made up history!!!
    from tbl4_mark_gap2
)
--- rewrite dbt_valid_from_2B to be the actual dbt_valid_from
, tbl4 as (
--    select  UNIQUE_ID_KEY, dbt_valid_from_2B as  dbt_valid_from, dbt_valid_to from  fill_gap
select *, dbt_valid_from_2B as  dbt_valid_from from  tbl4_fill_gap
)

    
--********************************************************************************
-----------------------------FINAL DATES TABLE union all dates, join to get values
--            unpivot dbt_valid_from, dbt_valid_to  into a single column adate, all other columns are gone
, combo_dates as (
    select UNIQUE_ID_KEY, adate from tbl0 unpivot( adate for headers in ( dbt_valid_from, dbt_valid_to )) 
                             union
              select UNIQUE_ID_KEY, adate from tbl1 unpivot( adate for headers in ( dbt_valid_from, dbt_valid_to )) 
                             union
              select UNIQUE_ID_KEY, adate from tbl2 unpivot( adate for headers in ( dbt_valid_from, dbt_valid_to )) 
                             union
              select UNIQUE_ID_KEY, adate from tbl3 unpivot( adate for headers in ( dbt_valid_from, dbt_valid_to )) 
                             union
              select UNIQUE_ID_KEY, adate from tbl4 unpivot( adate for headers in ( dbt_valid_from, dbt_valid_to )) 
)

--- remove duplicate dates with distinct
, unique_dates as (
    select distinct * from combo_dates order by adate    
)
--- convert single column back into 2 columns aka pivot
, final_dates as (
    select UNIQUE_ID_KEY, 
        adate as dbt_valid_from, 
        lead( adate, 1 ) over ( partition by  UNIQUE_ID_KEY order by adate ) dbt_valid_to 
from unique_dates  
)
    
----------------------JOIN ALL tables together with matching dates
, join_sql as ( 
select 
              tbl0.UNIQUE_ID_KEY
  , tbl0.STG_CUSTOMER_NAME_CUST_NM_NM
  , tbl1.STG_CUSTOMER_BLOCK_DOCUMENT_BLOCK_IND
  , tbl2.STG_CUSTOMER_BLOCK_THREATENING_BEHAVIOR_BLOCK_IND
  , tbl3.STG_CUSTOMER_ADDRESS_CNTRY_NM
  , tbl3.STG_CUSTOMER_ADDRESS_CUST_ADDR_CITY_NM
  , tbl3.STG_CUSTOMER_ADDRESS_CUST_ADDR_CNTY_NM
  , tbl3.STG_CUSTOMER_ADDRESS_CUST_ADDR_COMT
  , tbl3.STG_CUSTOMER_ADDRESS_CUST_ADDR_POST_CD
  , tbl3.STG_CUSTOMER_ADDRESS_CUST_ADDR_STR_1
  , tbl3.STG_CUSTOMER_ADDRESS_CUST_ADDR_STR_2
  , tbl3.STG_CUSTOMER_ADDRESS_CUST_ADDR_VLDT_IND
  , tbl3.STG_CUSTOMER_ADDRESS_STT_ABRV
  , tbl3.STG_CUSTOMER_ADDRESS_STT_NM
  , tbl3.MAILING_ADDRESS_EFFECTIVE_DATE
  , tbl3.MAILING_ADDRESS_END_DATE
  , tbl4.STG_CUSTOMER_ADDRESS_PHYS_CNTRY_NM
  , tbl4.STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_CITY_NM
  , tbl4.STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_CNTY_NM
  , tbl4.STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_COMT
  , tbl4.STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_POST_CD
  , tbl4.STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_STR_1
  , tbl4.STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_STR_2
  , tbl4.STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_VLDT_IND
  , tbl4.STG_CUSTOMER_ADDRESS_PHYS_STT_ABRV
  , tbl4.STG_CUSTOMER_ADDRESS_PHYS_STT_NM
  , tbl4.PHYSICAL_ADDRESS_EFFECTIVE_DATE
  , tbl4.PHYSICAL_ADDRESS_END_DATE
  , final_dates.dbt_valid_from
  , final_dates.dbt_valid_to
    from tbl0
        inner join final_dates
        on tbl0.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY 
        and tbl0.dbt_valid_from < final_dates.dbt_valid_to and tbl0.dbt_valid_to > final_dates.dbt_valid_from
              left join tbl1 on  tbl1.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY 
        and  tbl1.dbt_valid_from < final_dates.dbt_valid_to  and  tbl1.dbt_valid_to > final_dates.dbt_valid_from
              left join tbl2 on  tbl2.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY 
        and  tbl2.dbt_valid_from < final_dates.dbt_valid_to  and  tbl2.dbt_valid_to > final_dates.dbt_valid_from
              left join tbl3 on  tbl3.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY 
        and  tbl3.dbt_valid_from < final_dates.dbt_valid_to  and  tbl3.dbt_valid_to > final_dates.dbt_valid_from
              left join tbl4 on  tbl4.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY 
        and  tbl4.dbt_valid_from < final_dates.dbt_valid_to  and  tbl4.dbt_valid_to > final_dates.dbt_valid_from)
        
 ------------name back to std names
            , rename as (
                select distinct
                    UNIQUE_ID_KEY, --DEBUG_ID,
                    md5(coalesce(cast( UNIQUE_ID_KEY as varchar ), '') || '|' || coalesce(cast( CURRENT_TIMESTAMP as varchar ), '')) as dbt_scd_id,

                    --data cols
                    
                             STG_CUSTOMER_NAME_CUST_NM_NM as CUST_NM_NM , 
                             STG_CUSTOMER_BLOCK_DOCUMENT_BLOCK_IND as DOCUMENT_BLOCK_IND , 
                             STG_CUSTOMER_BLOCK_THREATENING_BEHAVIOR_BLOCK_IND as THREATENING_BEHAVIOR_BLOCK_IND , 
                             STG_CUSTOMER_ADDRESS_CNTRY_NM as CNTRY_NM , 
                             STG_CUSTOMER_ADDRESS_CUST_ADDR_CITY_NM as CUST_ADDR_CITY_NM , 
                             STG_CUSTOMER_ADDRESS_CUST_ADDR_CNTY_NM as CUST_ADDR_CNTY_NM , 
                             STG_CUSTOMER_ADDRESS_CUST_ADDR_COMT as CUST_ADDR_COMT , 
                             STG_CUSTOMER_ADDRESS_CUST_ADDR_POST_CD as CUST_ADDR_POST_CD , 
                             STG_CUSTOMER_ADDRESS_CUST_ADDR_STR_1 as CUST_ADDR_STR_1 , 
                             STG_CUSTOMER_ADDRESS_CUST_ADDR_STR_2 as CUST_ADDR_STR_2 , 
                             STG_CUSTOMER_ADDRESS_CUST_ADDR_VLDT_IND as CUST_ADDR_VLDT_IND , 
                             STG_CUSTOMER_ADDRESS_STT_ABRV as STT_ABRV , 
                             STG_CUSTOMER_ADDRESS_STT_NM as STT_NM , 
                             STG_CUSTOMER_ADDRESS_PHYS_CNTRY_NM as PHYS_CNTRY_NM , 
                             STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_CITY_NM as PHYS_CUST_ADDR_CITY_NM , 
                             STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_CNTY_NM as PHYS_CUST_ADDR_CNTY_NM , 
                             STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_COMT as PHYS_CUST_ADDR_COMT , 
                             STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_POST_CD as PHYS_CUST_ADDR_POST_CD , 
                             STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_STR_1 as PHYS_CUST_ADDR_STR_1 , 
                             STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_STR_2 as PHYS_CUST_ADDR_STR_2 , 
                             STG_CUSTOMER_ADDRESS_PHYS_CUST_ADDR_VLDT_IND as PHYS_CUST_ADDR_VLDT_IND , 
                             STG_CUSTOMER_ADDRESS_PHYS_STT_ABRV as PHYS_STT_ABRV , 
                             STG_CUSTOMER_ADDRESS_PHYS_STT_NM as PHYS_STT_NM ,
                             MAILING_ADDRESS_EFFECTIVE_DATE, 
                             MAILING_ADDRESS_END_DATE,
                             PHYSICAL_ADDRESS_EFFECTIVE_DATE,
                             PHYSICAL_ADDRESS_END_DATE,

                    to_timestamp_ntz( current_timestamp ) as dbt_updated_at,
                    --start end
                    dbt_valid_from,
                    NULLIF( dbt_valid_to, TO_DATE('12/31/2999') ) as dbt_valid_to
                    from join_sql
            )
   
--********************************************************************************----------------------------- CREATING COLLAPSE ID OF ALL TRACKED COLUMNS AND MAKING IT UNIQUE TO REMOVE DUPLICATE ROWS
        , RENAME_BUILD_GRANULARITY AS (
         SELECT *, MIN( UNIQUE_ID_KEY|| '~' 
                       ||coalesce(cast( CUST_NM_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( DOCUMENT_BLOCK_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( THREATENING_BEHAVIOR_BLOCK_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~'
||coalesce(cast( CNTRY_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( CUST_ADDR_CITY_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( CUST_ADDR_CNTY_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( CUST_ADDR_COMT AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( CUST_ADDR_POST_CD AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( CUST_ADDR_STR_1 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( CUST_ADDR_STR_2 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( CUST_ADDR_VLDT_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( STT_ABRV AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( STT_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CNTRY_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CUST_ADDR_CITY_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CUST_ADDR_CNTY_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CUST_ADDR_COMT AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CUST_ADDR_POST_CD AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CUST_ADDR_STR_1 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CUST_ADDR_STR_2 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CUST_ADDR_VLDT_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_STT_ABRV AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_STT_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~'
||coalesce(cast( MAILING_ADDRESS_EFFECTIVE_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( MAILING_ADDRESS_END_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~'
||coalesce(cast( PHYSICAL_ADDRESS_EFFECTIVE_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~'
||coalesce(cast( PHYSICAL_ADDRESS_END_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ))
      OVER( PARTITION BY UNIQUE_ID_KEY|| '~' 
          ||coalesce(cast( CUST_NM_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( DOCUMENT_BLOCK_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( THREATENING_BEHAVIOR_BLOCK_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~'
||coalesce(cast( CNTRY_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( CUST_ADDR_CITY_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( CUST_ADDR_CNTY_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( CUST_ADDR_COMT AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( CUST_ADDR_POST_CD AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( CUST_ADDR_STR_1 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( CUST_ADDR_STR_2 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( CUST_ADDR_VLDT_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( STT_ABRV AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( STT_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CNTRY_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CUST_ADDR_CITY_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CUST_ADDR_CNTY_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CUST_ADDR_COMT AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CUST_ADDR_POST_CD AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CUST_ADDR_STR_1 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CUST_ADDR_STR_2 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_CUST_ADDR_VLDT_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_STT_ABRV AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( PHYS_STT_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~'
||coalesce(cast( MAILING_ADDRESS_EFFECTIVE_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' 
||coalesce(cast( MAILING_ADDRESS_END_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~'
||coalesce(cast( PHYSICAL_ADDRESS_EFFECTIVE_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~'
||coalesce(cast( PHYSICAL_ADDRESS_END_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )) AS RENAME_COLLAPSE_ID
    FROM RENAME
    )

    , RENAME_ADD_LAG AS ( --- FIRST COLLAPSE
    SELECT
        *,
        ROW_NUMBER() OVER( PARTITION BY RENAME_COLLAPSE_ID ORDER BY DBT_VALID_FROM, DBT_VALID_TO) AS RN,
        LAG( DBT_VALID_TO, 1 ) OVER ( PARTITION BY RENAME_COLLAPSE_ID ORDER BY DBT_VALID_FROM, DBT_VALID_TO) AS PEND
    FROM
         RENAME_BUILD_GRANULARITY
    )
    --- MARK ALL ROWS THAT HAVE DUPLICATE VALUES FOR A RANGE OF DATES BASED ON THE ROW NUMBER THAT STARTS THE DATES
--                                GAPID(STARTING ROW NUMBER)
--                                    SUM
-- JAN1,JAN 2 A  ->  JAN1,JAN2 A   23 23
-- JAN2,JAN 3 A  ->  JAN2,JAN3 A   0  23
-- JAN3,JAN 4 A  ->  JAN3,JAN4 A   0  23

    , RENAME_ADD_GAPID AS (
    SELECT 
            *,
            CASE WHEN DBT_VALID_FROM > DATEADD( DAY, 1, RENAME_ADD_LAG.PEND ) THEN RN ELSE 0 END AS GAP,
            SUM( GAP ) OVER ( PARTITION BY RENAME_COLLAPSE_ID ORDER BY RENAME_ADD_LAG.RN ) AS GAPID

--- GROUP ON THE GAPID TO REMOVE DUPLICATE ROWS AND RESET START AND END DATES IN A CRISS-CROSS(UPPER LEFT, TO BOTTOM RIGHT)
--- RESULT ABOVE BECOMES: JAN1,JAN 4 A  


    FROM RENAME_ADD_LAG )
    --- COLLAPSE EVERY ROW WITH SAME GAPID INTO ONE ROW 
    , RENAME_COLLAPSE AS 

        ( 
        SELECT 
        RENAME_COLLAPSE_ID, MIN( DBT_VALID_FROM ) AS DBT_VALID_FROM, MAX( DBT_VALID_TO ) AS DBT_VALID_TO
        FROM RENAME_ADD_GAPID
        GROUP BY RENAME_COLLAPSE_ID, GAPID
         )
---- GROUP BY REMOVES THE COLUMNS, USE SPLIT_PART TO GET THE VALUES AND COLUMNS BACK               
    , RENAME_GRANULARITY AS (
        SELECT     
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~',1 ),'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS UNIQUE_ID_KEY, 
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 2), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CUST_NM_NM, 
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 3), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS DOCUMENT_BLOCK_IND, 
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 4), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS THREATENING_BEHAVIOR_BLOCK_IND, 
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 5), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CNTRY_NM, 
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 6), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CUST_ADDR_CITY_NM, 
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 7), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CUST_ADDR_CNTY_NM, 
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 8), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CUST_ADDR_COMT, 
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 9), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CUST_ADDR_POST_CD, 
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 10), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CUST_ADDR_STR_1,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 11), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CUST_ADDR_STR_2,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 12), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CUST_ADDR_VLDT_IND,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 13), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS STT_ABRV,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 14), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS STT_NM,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 15), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYS_CNTRY_NM,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 16), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYS_CUST_ADDR_CITY_NM,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 17), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYS_CUST_ADDR_CNTY_NM,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 18), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYS_CUST_ADDR_COMT,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 19), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYS_CUST_ADDR_POST_CD,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 20), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYS_CUST_ADDR_STR_1,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 21), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYS_CUST_ADDR_STR_2,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 22), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYS_CUST_ADDR_VLDT_IND,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 23), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYS_STT_ABRV,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 24), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYS_STT_NM,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 25), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_ADDRESS_EFFECTIVE_DATE,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 26), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_ADDRESS_END_DATE,      
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 27), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_ADDRESS_EFFECTIVE_DATE,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 28), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_ADDRESS_END_DATE, *
        FROM RENAME_COLLAPSE
    )

, final_sql as ( select 
              UNIQUE_ID_KEY
              , md5(coalesce(cast( UNIQUE_ID_KEY as varchar ), '') || '|' || coalesce(cast( CURRENT_TIMESTAMP as varchar ), '')) as dbt_scd_id
              , to_timestamp( DBT_VALID_FROM ) as DBT_VALID_FROM
              , to_timestamp( case when  ( row_number() over ( partition by UNIQUE_ID_KEY order by UNIQUE_ID_KEY, DBT_VALID_FROM DESC, DBT_VALID_TO DESC)) =1 THEN NULL else  DBT_VALID_TO end )  AS DBT_VALID_TO
              , to_timestamp( current_timestamp ) as DBT_UPDATED_AT
              , CUST_NM_NM AS CUSTOMER_NAME
              , NVL(DOCUMENT_BLOCK_IND,'N')AS DOCUMENT_BLOCK_IND 
              , NVL(THREATENING_BEHAVIOR_BLOCK_IND,'N') as THREAT_BEHAVIOR_BLOCK_IND
              , CUST_ADDR_STR_1 as MAILING_STREET_ADDRESS_1
              , CUST_ADDR_STR_2 as MAILING_STREET_ADDRESS_2
              , CUST_ADDR_CITY_NM as MAILING_ADDRESS_CITY_NAME
              , STT_ABRV as MAILING_ADDRESS_STATE_CODE
              , STT_NM as MAILING_ADDRESS_STATE_NAME
              , CUST_ADDR_POST_CD as MAILING_ADDRESS_POSTAL_CODE
              , CUST_ADDR_CNTY_NM as MAILING_ADDRESS_COUNTY_NAME
              , CNTRY_NM as MAILING_ADDRESS_COUNTRY_NAME
              , CUST_ADDR_VLDT_IND as MAILING_ADDRESS_VALIDATED_IND
              , CUST_ADDR_COMT as MAILING_ADDRESS_COMMENT_TEXT
              , PHYS_CUST_ADDR_STR_1 as PHYSICAL_STREET_ADDRESS_1
              , PHYS_CUST_ADDR_STR_2 as PHYSICAL_STREET_ADDRESS_2
              , PHYS_CUST_ADDR_CITY_NM as PHYSICAL_ADDRESS_CITY_NAME
              , PHYS_STT_ABRV as PHYSICAL_ADDRESS_STATE_CODE
              , PHYS_STT_NM as PHYSICAL_ADDRESS_STATE_NAME
              , PHYS_CUST_ADDR_POST_CD as PHYSICAL_ADDRESS_POSTAL_CODE
              , PHYS_CUST_ADDR_CNTY_NM as PHYSICAL_ADDRESS_COUNTY_NAME
              , PHYS_CNTRY_NM as PHYSICAL_ADDRESS_COUNTRY_NAME
              , PHYS_CUST_ADDR_VLDT_IND as PHYSICAL_ADDRESS_VALIDATED_IND
              , PHYS_CUST_ADDR_COMT as PHYSICAL_ADDRESS_COMMENT_TEXT
              , CASE WHEN LENGTH(MAILING_ADDRESS_POSTAL_CODE) = 9 
              THEN LEFT(MAILING_ADDRESS_POSTAL_CODE, 5) ||'-'|| RIGHT(MAILING_ADDRESS_POSTAL_CODE, 4) 
              ELSE MAILING_ADDRESS_POSTAL_CODE END AS MAILING_FORMATTED_ADDRESS_POSTAL_CODE
		      , CASE WHEN LENGTH(MAILING_ADDRESS_POSTAL_CODE) = 9 THEN LEFT(MAILING_ADDRESS_POSTAL_CODE, 5) ELSE MAILING_ADDRESS_POSTAL_CODE END AS MAILING_FORMATTED_ADDRESS_ZIP_CODE
		      , CASE WHEN LENGTH(MAILING_ADDRESS_POSTAL_CODE) = 9 THEN RIGHT(MAILING_ADDRESS_POSTAL_CODE, 4) ELSE NULL END AS MAILING_FORMATTED_ADDRESS_ZIP4_CODE
              , CASE WHEN LENGTH(PHYSICAL_ADDRESS_POSTAL_CODE) = 9 
              THEN LEFT(PHYSICAL_ADDRESS_POSTAL_CODE, 5) ||'-'|| RIGHT(PHYSICAL_ADDRESS_POSTAL_CODE, 4) 
              ELSE PHYSICAL_ADDRESS_POSTAL_CODE END AS PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE
		      , CASE WHEN LENGTH(PHYSICAL_ADDRESS_POSTAL_CODE) = 9 THEN LEFT(PHYSICAL_ADDRESS_POSTAL_CODE, 5) ELSE PHYSICAL_ADDRESS_POSTAL_CODE END AS PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE
		      , CASE WHEN LENGTH(PHYSICAL_ADDRESS_POSTAL_CODE) = 9 THEN RIGHT(PHYSICAL_ADDRESS_POSTAL_CODE, 4) ELSE NULL END AS PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE
--//              ,MAILING_ADDRESS_EFFECTIVE_DATE
--//              ,MAILING_ADDRESS_END_DATE
--//              ,PHYSICAL_ADDRESS_EFFECTIVE_DATE             
--//              ,PHYSICAL_ADDRESS_END_DATE
              from rename_granularity  order by dbt_valid_from, dbt_valid_to )
    

--------PHASE 1: DEBUG
-- set Debug to True to test these

select *  from final_sql
      );
    