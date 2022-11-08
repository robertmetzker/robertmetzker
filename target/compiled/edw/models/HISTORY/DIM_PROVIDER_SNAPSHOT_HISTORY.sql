WITH

ETL_STEP1 AS (
select concat(PRVDR_BASE_NMBR,PRVDR_SFX_NMBR) peach_no, PRVDR_BASE_NMBR,PRVDR_SFX_NMBR, PRVDR_LEGAL_NAME
, CONDITIONAL_CHANGE_EVENT(NVL(PRVDR_LEGAL_NAME, 'NA')) OVER(PARTITION BY PRVDR_BASE_NMBR,PRVDR_SFX_NMBR order by PRDT_CRT_DTTM,DCTVT_DTTM) AS CCE
, PRDT_CRT_DTTM,DCTVT_DTTM
from   STAGING.STG_TMPPRDT  

QUALIFY (ROW_NUMBER() OVER(PARTITION BY PRVDR_BASE_NMBR,PRVDR_SFX_NMBR, PRDT_CRT_DTTM::DATE ORDER BY PRDT_CRT_DTTM DESC,DCTVT_DTTM DESC))=1
), 


--********************************************************************************
-----------------------------setting up generic table tbl0_setup
---- rename to generic col names and select cols for history
tbl0_setup1 as (
select distinct 
		md5( coalesce(cast( PRVDR_BASE_NMBR as varchar ), '' )|| '-' || coalesce(cast( PRVDR_SFX_NMBR as varchar ), '' ) ) as UNIQUE_ID_KEY,
    --data cols
    	
		upper( TRIM( PRVDR_LEGAL_NAME ) ) as STG_TMPPRDT_PRVDR_LEGAL_NAME
,

    --start end cols
    
        MIN(IFF( IFNULL( try_to_timestamp( PRDT_CRT_DTTM::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), PRDT_CRT_DTTM ))
          as dbt_valid_from_initial_1,
    
        MAX(IFF( IFNULL( try_to_timestamp( DCTVT_DTTM::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), DCTVT_DTTM ))
          as dbt_valid_to_1 

    from ETL_STEP1
  group by 1,2, CCE
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
        

--********************************************************************************----------------------------- creating collapse id of all tracked columns and making it unique to remove duplicate rows
    , tbl0_setup_build_granularity as (
        select *, min( UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_TMPPRDT_PRVDR_LEGAL_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' ) ) OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_TMPPRDT_PRVDR_LEGAL_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )) as tbl0_setup_collapse_id
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
        select     nullif(split_part( tbl0_setup_collapse_id, '~',1 ),'THISISANULLVALplaceholdertoberemoved' ) as UNIQUE_ID_KEY, nullif(split_part( tbl0_setup_collapse_id, '~', 2), 'THISISANULLVALplaceholdertoberemoved' ) as STG_TMPPRDT_PRVDR_LEGAL_NAME,  *
        from tbl0_setup_collapse
    )



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

    ,
 ETL_STEP2 AS (
select concat(PRVDR_BASE_NMBR,PRVDR_SFX_NMBR) peach_no, PRVDR_BASE_NMBR,PRVDR_SFX_NMBR, DBA_NAME, NEW_PATIENT_CODE, NEW_PATIENT_DESC
, CONDITIONAL_CHANGE_EVENT( array_to_string(array_construct_compact(DBA_NAME, NEW_PATIENT_CODE, NEW_PATIENT_DESC ),'')) 
                                    OVER(PARTITION BY PRVDR_BASE_NMBR,PRVDR_SFX_NMBR order by PRED_CRT_DTTM,DCTVT_DTTM) AS CCE
, PRED_CRT_DTTM,DCTVT_DTTM
from  STAGING.STG_TMPPRED

QUALIFY(ROW_NUMBER() OVER(PARTITION BY PRVDR_BASE_NMBR,PRVDR_SFX_NMBR, PRED_CRT_DTTM::DATE ORDER BY PRED_CRT_DTTM DESC,DCTVT_DTTM DESC))=1
order by PRED_CRT_DTTM,DCTVT_DTTM), 
--********************************************************************************
-----------------------------setting up generic table tbl1_setup
---- rename to generic col names and select cols for history
tbl1_setup1 as (
select distinct 
		md5( coalesce(cast( PRVDR_BASE_NMBR as varchar ), '' )|| '-' || coalesce(cast( PRVDR_SFX_NMBR as varchar ), '' ) ) as UNIQUE_ID_KEY,
    --data cols
    	
		DBA_NAME as STG_TMPPRED_DBA_NAME, 
		NEW_PATIENT_CODE as STG_TMPPRED_NEW_PATIENT_CODE, 
		NEW_PATIENT_DESC as STG_TMPPRED_NEW_PATIENT_DESC
,

    --start end cols
    
        MIN(IFF( IFNULL( try_to_timestamp( PRED_CRT_DTTM::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), PRED_CRT_DTTM ))
          as dbt_valid_from_initial_1,
    
        MAX(IFF( IFNULL( try_to_timestamp( DCTVT_DTTM::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), DCTVT_DTTM ))
          as dbt_valid_to_1 

    from ETL_STEP2
  GROUP BY 1,2,3,4, CCE
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
        select *, min( UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_TMPPRED_DBA_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_TMPPRED_NEW_PATIENT_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_TMPPRED_NEW_PATIENT_DESC as varchar ), 'THISISANULLVALplaceholdertoberemoved' ) ) OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_TMPPRED_DBA_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_TMPPRED_NEW_PATIENT_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_TMPPRED_NEW_PATIENT_DESC as varchar ), 'THISISANULLVALplaceholdertoberemoved' )) as tbl1_setup_collapse_id
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
        select     nullif(split_part( tbl1_setup_collapse_id, '~',1 ),'THISISANULLVALplaceholdertoberemoved' ) as UNIQUE_ID_KEY, nullif(split_part( tbl1_setup_collapse_id, '~', 2), 'THISISANULLVALplaceholdertoberemoved' ) as STG_TMPPRED_DBA_NAME, nullif(split_part( tbl1_setup_collapse_id, '~', 3), 'THISISANULLVALplaceholdertoberemoved' ) as STG_TMPPRED_NEW_PATIENT_CODE, nullif(split_part( tbl1_setup_collapse_id, '~', 4), 'THISISANULLVALplaceholdertoberemoved' ) as STG_TMPPRED_NEW_PATIENT_DESC,  *
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
)

    ,
--********************************************************************************
-----------------------------setting up generic table tbl2_setup
---- rename to generic col names and select cols for history
tbl2_setup1 as (
select distinct 
		md5( coalesce(LEFT(cast( PRO_NUM as varchar ), 7), '' )|| '-' || coalesce(RIGHT(cast( PRO_NUM as varchar ),4), '' ) )as UNIQUE_ID_KEY,
    --data cols
    	
		CRITICAL_ACCESS_IND as STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_CRITICAL_ACCESS_IND, 
		MEDICARE_PROVIDER_TYPE_CODE as STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_MEDICARE_PROVIDER_TYPE_CODE, 
		MEDICARE_PROVIDER_TYPE_DESC as STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_MEDICARE_PROVIDER_TYPE_DESC, 
		OPPS_QUALITY_IND as STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_OPPS_QUALITY_IND, 
		PROVIDER_CBSA_CODE as STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_PROVIDER_CBSA_CODE
,

    --start end cols
    
        IFF( IFNULL( try_to_timestamp( EFFECTIVE_DATE::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), EFFECTIVE_DATE )
          as dbt_valid_from_initial_1,
    
        IFF( IFNULL( try_to_timestamp( EXPIRATION_DATE::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), EXPIRATION_DATE )
          as dbt_valid_to_1 

    from  STAGING.STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION
    
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
        select *, min( UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_MEDICARE_PROVIDER_TYPE_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_MEDICARE_PROVIDER_TYPE_DESC as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_OPPS_QUALITY_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_CRITICAL_ACCESS_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_PROVIDER_CBSA_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' ) ) OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_MEDICARE_PROVIDER_TYPE_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_MEDICARE_PROVIDER_TYPE_DESC as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_OPPS_QUALITY_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_CRITICAL_ACCESS_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_PROVIDER_CBSA_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )) as tbl2_setup_collapse_id
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
        select     nullif(split_part( tbl2_setup_collapse_id, '~',1 ),'THISISANULLVALplaceholdertoberemoved' ) as UNIQUE_ID_KEY, nullif(split_part( tbl2_setup_collapse_id, '~', 2), 'THISISANULLVALplaceholdertoberemoved' ) as STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_MEDICARE_PROVIDER_TYPE_CODE, nullif(split_part( tbl2_setup_collapse_id, '~', 3), 'THISISANULLVALplaceholdertoberemoved' ) as STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_MEDICARE_PROVIDER_TYPE_DESC, nullif(split_part( tbl2_setup_collapse_id, '~', 4), 'THISISANULLVALplaceholdertoberemoved' ) as STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_OPPS_QUALITY_IND, nullif(split_part( tbl2_setup_collapse_id, '~', 5), 'THISISANULLVALplaceholdertoberemoved' ) as STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_CRITICAL_ACCESS_IND, nullif(split_part( tbl2_setup_collapse_id, '~', 6), 'THISISANULLVALplaceholdertoberemoved' ) as STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_PROVIDER_CBSA_CODE,  *
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
		md5( coalesce(cast( PRVDR_BASE_NMBR as varchar ), '' )|| '-' || coalesce(cast( PRVDR_SFX_NMBR as varchar ), '' ) ) as UNIQUE_ID_KEY,
    --data cols
    	
		PROGRAM_PARTICIPATION_PERIOD as STG_PROVIDER_PROGRAM_FOCUS_PROGRAM_PARTICIPATION_PERIOD, 
		PROVIDER_PROGRAM_CODE as STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_CODE, 
		PROVIDER_PROGRAM_DESC as STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_DESC, 
		PROVIDER_PROGRAM_FOCUS_TEXT as STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_FOCUS_TEXT
,

    --start end cols
    
        IFF( IFNULL( try_to_timestamp( EFCTV_DATE::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), EFCTV_DATE )
          as dbt_valid_from_initial_1,
    
        IFF( IFNULL( try_to_timestamp( ENDNG_DATE::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), ENDNG_DATE )
          as dbt_valid_to_1 

    from  STAGING.STG_PROVIDER_PROGRAM_FOCUS
    where DCTVT_DTTM > CURRENT_DATE()

    QUALIFY(ROW_NUMBER() OVER(PARTITION BY PRVDR_BASE_NMBR,PRVDR_SFX_NMBR, CRT_DTTM::DATE ORDER BY CRT_DTTM DESC, EFCTV_DATE DESC)) = 1
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
    , tbl3_setup_build_granularity as (
        select *, min( UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_DESC as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_FOCUS_TEXT as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_PROVIDER_PROGRAM_FOCUS_PROGRAM_PARTICIPATION_PERIOD as varchar ), 'THISISANULLVALplaceholdertoberemoved' ) ) OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_DESC as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_FOCUS_TEXT as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_PROVIDER_PROGRAM_FOCUS_PROGRAM_PARTICIPATION_PERIOD as varchar ), 'THISISANULLVALplaceholdertoberemoved' )) as tbl3_setup_collapse_id
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
        tbl3_setup_collapse_id, min( dbt_valid_from_initial ) as dbt_valid_from_initial, max( dbt_valid_to ) as dbt_valid_to
        from tbl3_setup_add_gapid
        group by tbl3_setup_collapse_id, gapid
         )
---- group by removes the columns, use split_part to get the values and columns back               
    , tbl3_setup_granularity as (
        select     nullif(split_part( tbl3_setup_collapse_id, '~',1 ),'THISISANULLVALplaceholdertoberemoved' ) as UNIQUE_ID_KEY, nullif(split_part( tbl3_setup_collapse_id, '~', 2), 'THISISANULLVALplaceholdertoberemoved' ) as STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_CODE, nullif(split_part( tbl3_setup_collapse_id, '~', 3), 'THISISANULLVALplaceholdertoberemoved' ) as STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_DESC, nullif(split_part( tbl3_setup_collapse_id, '~', 4), 'THISISANULLVALplaceholdertoberemoved' ) as STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_FOCUS_TEXT, nullif(split_part( tbl3_setup_collapse_id, '~', 5), 'THISISANULLVALplaceholdertoberemoved' ) as STG_PROVIDER_PROGRAM_FOCUS_PROGRAM_PARTICIPATION_PERIOD,  *
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

    
--********************************************************************************
-----------------------------FINAL DATES TABLE union all dates, join to get values
--	unpivot dbt_valid_from, dbt_valid_to  into a single column adate, all other columns are gone
, combo_dates as (
    select UNIQUE_ID_KEY, adate from tbl0 unpivot( adate for headers in ( dbt_valid_from, dbt_valid_to )) 
		union
	select UNIQUE_ID_KEY, adate from tbl1 unpivot( adate for headers in ( dbt_valid_from, dbt_valid_to )) 
		union
	select UNIQUE_ID_KEY, adate from tbl2 unpivot( adate for headers in ( dbt_valid_from, dbt_valid_to )) 
		union
	select UNIQUE_ID_KEY, adate from tbl3 unpivot( adate for headers in ( dbt_valid_from, dbt_valid_to )) 
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
	tbl0.UNIQUE_ID_KEY,
	tbl0.STG_TMPPRDT_PRVDR_LEGAL_NAME, tbl1.STG_TMPPRED_DBA_NAME, tbl1.STG_TMPPRED_NEW_PATIENT_CODE, tbl1.STG_TMPPRED_NEW_PATIENT_DESC, tbl2.STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_CRITICAL_ACCESS_IND, tbl2.STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_MEDICARE_PROVIDER_TYPE_CODE, tbl2.STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_MEDICARE_PROVIDER_TYPE_DESC, tbl2.STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_OPPS_QUALITY_IND, tbl2.STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_PROVIDER_CBSA_CODE, tbl3.STG_PROVIDER_PROGRAM_FOCUS_PROGRAM_PARTICIPATION_PERIOD, tbl3.STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_CODE, tbl3.STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_DESC, tbl3.STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_FOCUS_TEXT, 
	final_dates.dbt_valid_from, final_dates.dbt_valid_to
    from tbl0
        inner join final_dates
        on tbl0.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY 
        and tbl0.dbt_valid_from < final_dates.dbt_valid_to and tbl0.dbt_valid_to > final_dates.dbt_valid_from
            
	left join tbl1 on  tbl1.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY 
        and  tbl1.dbt_valid_from < final_dates.dbt_valid_to  and  tbl1.dbt_valid_to > final_dates.dbt_valid_from
	left join tbl2 on  tbl2.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY 
        and  tbl2.dbt_valid_from < final_dates.dbt_valid_to  and  tbl2.dbt_valid_to > final_dates.dbt_valid_from
	left join tbl3 on  tbl3.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY 
        and  tbl3.dbt_valid_from < final_dates.dbt_valid_to  and  tbl3.dbt_valid_to > final_dates.dbt_valid_from)
        ------------name back to std names
            , rename as (
                select distinct
                    UNIQUE_ID_KEY, --DEBUG_ID,
                    md5(coalesce(cast( UNIQUE_ID_KEY as varchar ), '') || '|' || coalesce(cast( CURRENT_TIMESTAMP as varchar ), '')) as dbt_scd_id,

                    --data cols
                    
		STG_TMPPRDT_PRVDR_LEGAL_NAME as PRVDR_LEGAL_NAME , 
		STG_TMPPRED_DBA_NAME as DBA_NAME , 
		STG_TMPPRED_NEW_PATIENT_CODE as NEW_PATIENT_CODE , 
		STG_TMPPRED_NEW_PATIENT_DESC as NEW_PATIENT_DESC , 
		STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_CRITICAL_ACCESS_IND as CRITICAL_ACCESS_IND , 
		STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_MEDICARE_PROVIDER_TYPE_CODE as MEDICARE_PROVIDER_TYPE_CODE , 
		STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_MEDICARE_PROVIDER_TYPE_DESC as MEDICARE_PROVIDER_TYPE_DESC , 
		STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_OPPS_QUALITY_IND as OPPS_QUALITY_IND , 
		STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION_PROVIDER_CBSA_CODE as PROVIDER_CBSA_CODE , 
		STG_PROVIDER_PROGRAM_FOCUS_PROGRAM_PARTICIPATION_PERIOD as PROGRAM_PARTICIPATION_PERIOD , 
		STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_CODE as PROVIDER_PROGRAM_CODE , 
		STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_DESC as PROVIDER_PROGRAM_DESC , 
		STG_PROVIDER_PROGRAM_FOCUS_PROVIDER_PROGRAM_FOCUS_TEXT as PROVIDER_PROGRAM_FOCUS_TEXT ,

                    to_timestamp_ntz( current_timestamp ) as dbt_updated_at,
                    --start end
                    dbt_valid_from,
                    NULLIF( dbt_valid_to, TO_DATE('12/31/2999') ) as dbt_valid_to
                    from join_sql
            )
    

--********************************************************************************----------------------------- CREATING COLLAPSE ID OF ALL TRACKED COLUMNS AND MAKING IT UNIQUE TO REMOVE DUPLICATE ROWS
    , RENAME_BUILD_GRANULARITY AS (
        SELECT *, MIN( UNIQUE_ID_KEY|| '~' ||COALESCE(CAST( PRVDR_LEGAL_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( DBA_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( NEW_PATIENT_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( NEW_PATIENT_DESC AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( MEDICARE_PROVIDER_TYPE_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( MEDICARE_PROVIDER_TYPE_DESC AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( OPPS_QUALITY_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( CRITICAL_ACCESS_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( PROVIDER_CBSA_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( PROVIDER_PROGRAM_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( PROVIDER_PROGRAM_DESC AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( PROVIDER_PROGRAM_FOCUS_TEXT AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( PROGRAM_PARTICIPATION_PERIOD AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) ) OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||COALESCE(CAST( PRVDR_LEGAL_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( DBA_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( NEW_PATIENT_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( NEW_PATIENT_DESC AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( MEDICARE_PROVIDER_TYPE_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( MEDICARE_PROVIDER_TYPE_DESC AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( OPPS_QUALITY_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( CRITICAL_ACCESS_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( PROVIDER_CBSA_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( PROVIDER_PROGRAM_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( PROVIDER_PROGRAM_DESC AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( PROVIDER_PROGRAM_FOCUS_TEXT AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||COALESCE(CAST( PROGRAM_PARTICIPATION_PERIOD AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )) AS RENAME_COLLAPSE_ID
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
            SUM( GAP ) OVER ( PARTITION BY RENAME_COLLAPSE_ID ORDER BY RENAME_ADD_LAG.RN ) AS GAPID, 
      CONDITIONAL_CHANGE_EVENT(DBT_VALID_FROM) OVER (PARTITION BY UNIQUE_ID_KEY ORDER BY DBT_VALID_FROM, DBT_VALID_TO) AS CCE

--- GROUP ON THE GAPID TO REMOVE DUPLICATE ROWS AND RESET START AND END DATES IN A CRISS-CROSS(UPPER LEFT, TO BOTTOM RIGHT)
--- RESULT ABOVE BECOMES: JAN1,JAN 4 A  


    FROM RENAME_ADD_LAG )
    --- COLLAPSE EVERY ROW WITH SAME GAPID INTO ONE ROW 
    , RENAME_COLLAPSE AS 

        ( 
        SELECT 
        RENAME_COLLAPSE_ID, MIN( DBT_VALID_FROM ) AS DBT_VALID_FROM, MAX( DBT_VALID_TO ) AS DBT_VALID_TO
        FROM RENAME_ADD_GAPID
        GROUP BY RENAME_COLLAPSE_ID, GAPID, CCE
         )
---- GROUP BY REMOVES THE COLUMNS, USE SPLIT_PART TO GET THE VALUES AND COLUMNS BACK               
    , RENAME_GRANULARITY AS (
        SELECT     NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~',1 ),'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS UNIQUE_ID_KEY
      , NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 2), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PRVDR_LEGAL_NAME
      , NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 3), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS DBA_NAME
      , NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 4), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS NEW_PATIENT_CODE
      , NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 5), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS NEW_PATIENT_DESC
      , NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 6), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MEDICARE_PROVIDER_TYPE_CODE
      , NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 7), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MEDICARE_PROVIDER_TYPE_DESC
      , NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 8), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS OPPS_QUALITY_IND
      , NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 9), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CRITICAL_ACCESS_IND
      , NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 10), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PROVIDER_CBSA_CODE
      , NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 11), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PROVIDER_PROGRAM_CODE
      , NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 12), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PROVIDER_PROGRAM_DESC
      , NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 13), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PROVIDER_PROGRAM_FOCUS_TEXT
      , NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 14), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PROGRAM_PARTICIPATION_PERIOD,  *
        FROM RENAME_COLLAPSE
    )



, final_sql as ( select 
	UNIQUE_ID_KEY
	, md5(coalesce(cast( UNIQUE_ID_KEY as varchar ), '') || '|' || coalesce(cast( CURRENT_TIMESTAMP as varchar ), '')) as dbt_scd_id
	, to_timestamp( DBT_VALID_FROM ) as DBT_VALID_FROM
	, to_timestamp( case when  ( row_number() over ( partition by UNIQUE_ID_KEY order by UNIQUE_ID_KEY, DBT_VALID_FROM DESC, DBT_VALID_TO DESC)) =1 
                   THEN NULL else  DBT_VALID_TO end )  AS DBT_VALID_TO
	, PRVDR_LEGAL_NAME AS PROVIDER_NAME
	, DBA_NAME AS PROVIDER_DBA_SORT_NAME
	, NEW_PATIENT_CODE
	, NEW_PATIENT_DESC
	, MEDICARE_PROVIDER_TYPE_CODE
	, MEDICARE_PROVIDER_TYPE_DESC
	, OPPS_QUALITY_IND
	, CRITICAL_ACCESS_IND
	, PROVIDER_CBSA_CODE
	, PROVIDER_PROGRAM_CODE
	, PROVIDER_PROGRAM_DESC
	, PROVIDER_PROGRAM_FOCUS_TEXT
	, PROGRAM_PARTICIPATION_PERIOD 
from rename_granularity 

QUALIFY(ROW_NUMBER()OVER(PARTITION BY UNIQUE_ID_KEY, dbt_valid_from::DATE ORDER BY dbt_valid_from DESC, COALESCE(dbt_valid_to,'12/31/2999') DESC
                                , PROVIDER_NAME DESC, PROVIDER_DBA_SORT_NAME DESC, NEW_PATIENT_CODE DESC, NEW_PATIENT_DESC DESC, MEDICARE_PROVIDER_TYPE_CODE DESC, MEDICARE_PROVIDER_TYPE_DESC DESC
                                , OPPS_QUALITY_IND DESC, CRITICAL_ACCESS_IND DESC, PROVIDER_CBSA_CODE DESC, PROVIDER_PROGRAM_CODE DESC, PROVIDER_PROGRAM_DESC DESC, PROVIDER_PROGRAM_FOCUS_TEXT DESC
                                , PROGRAM_PARTICIPATION_PERIOD DESC)) = 1)
    
-- ETL_SORT
, ETL_SORT AS( SELECT * 
            , CONDITIONAL_CHANGE_EVENT(array_to_string(array_construct_compact(PROVIDER_NAME,PROVIDER_DBA_SORT_NAME,MEDICARE_PROVIDER_TYPE_CODE
                                        ,MEDICARE_PROVIDER_TYPE_DESC,CRITICAL_ACCESS_IND,OPPS_QUALITY_IND,PROVIDER_CBSA_CODE
                                       ,NEW_PATIENT_CODE,NEW_PATIENT_DESC,PROVIDER_PROGRAM_CODE,PROVIDER_PROGRAM_DESC
                                       ,PROVIDER_PROGRAM_FOCUS_TEXT,PROGRAM_PARTICIPATION_PERIOD ),'')
                                      ) 
                        OVER(PARTITION BY UNIQUE_ID_KEY ORDER BY DBT_VALID_FROM, COALESCE(DBT_VALID_TO, '12/31/2999')) AS CCE
            FROM final_sql)
            

--ETL LAYER
, ETL3 AS(
    select 
    UNIQUE_ID_KEY
    , PROVIDER_NAME
	, PROVIDER_DBA_SORT_NAME
	, NEW_PATIENT_CODE
	, NEW_PATIENT_DESC
	, MEDICARE_PROVIDER_TYPE_CODE
	, MEDICARE_PROVIDER_TYPE_DESC
	, OPPS_QUALITY_IND
	, CRITICAL_ACCESS_IND
	, PROVIDER_CBSA_CODE
	, PROVIDER_PROGRAM_CODE
	, PROVIDER_PROGRAM_DESC
	, PROVIDER_PROGRAM_FOCUS_TEXT
	, PROGRAM_PARTICIPATION_PERIOD 
    , MIN(DBT_VALID_FROM) AS DBT_VALID_FROM
    , NULLIF(MAX(NVL(DBT_VALID_TO,'9999-12-31'::DATE)), '9999-12-31') AS DBT_VALID_TO
    , to_timestamp( current_timestamp ) as DBT_UPDATED_AT
    
 from ETL_SORT
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14, CCE
ORDER BY UNIQUE_ID_KEY, dbt_valid_from
)

SELECT *
, md5(coalesce(cast( UNIQUE_ID_KEY as varchar ), '') || '|' || coalesce(cast( CURRENT_TIMESTAMP as varchar ), '')) as dbt_scd_id
FROM ETL3