with

--********************************************************************************
-----------------------------setting up generic table tbl0_setup
---- rename to generic col names and select cols for history
tbl0_setup01 as (
select distinct 
		md5( coalesce(cast( CUST_ID as varchar ), '' ) ) as UNIQUE_ID_KEY,
    --data cols
    	
		MAR_STS_TYP_NM as STG_PERSON_HISTORY_MAR_STS_TYP_NM
,

    --start end cols
    
        IFF( IFNULL( try_to_timestamp( HIST_EFF_DTM::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), HIST_EFF_DTM )
          as dbt_valid_from_initial_1,
    
        IFF( IFNULL( try_to_timestamp( HIST_END_DTM::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), HIST_END_DTM )
          as dbt_valid_to_1 
, CONDITIONAL_CHANGE_EVENT(MAR_STS_TYP_NM) OVER(PARTITION BY CUST_ID ORDER BY dbt_valid_from_initial_1,dbt_valid_to_1 ) AS CCE
    from STAGING.STG_PERSON_HISTORY
    WHERE  HIST_EFF_DTM::DATE <> COALESCE(HIST_END_DTM, '12/31/2099')::DATE
        )
-- ADDITIONAL STEP FOR HANDLING PERSON MULYIPLIERS
, tbl0_setup1 as ( SELECT UNIQUE_ID_KEY, STG_PERSON_HISTORY_MAR_STS_TYP_NM, MIN(dbt_valid_from_initial_1) dbt_valid_from_initial_1
                , MAX(dbt_valid_to_1) dbt_valid_to_1
                FROM tbl0_setup01 GROUP BY UNIQUE_ID_KEY, STG_PERSON_HISTORY_MAR_STS_TYP_NM,CCE)
---- only select latest values for a day
, tbl0_setup2 as (
    select *, 
           ROW_NUMBER () OVER ( PARTITION BY UNIQUE_ID_KEY, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) ORDER BY DBT_VALID_FROM_INITIAL_1 DESC ) AS tbl0_ROWN 
    from     tbl0_setup1 
    -- WHERE (STG_PERSON_HISTORY_MAR_STS_TYP_NM IS NOT NULL OR ROWN=1)
            qualify tbl0_ROWN = 1
        )
---- convert to a date, for comparisons across tables time differences do not matter, need to convert to ts at the end for dbt
, tbl0_setup as (
    select *, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) as DBT_VALID_FROM_INITIAL, TO_DATE( dbt_valid_to_1 ) as dbt_valid_to
    from     tbl0_setup2 
        )        
        

--********************************************************************************----------------------------- creating collapse id of all tracked columns and making it unique to remove duplicate rows
    , tbl0_setup_build_granularity as (
        select *, min( UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_PERSON_HISTORY_MAR_STS_TYP_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' ) ) OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_PERSON_HISTORY_MAR_STS_TYP_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )) as tbl0_setup_collapse_id
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
        select     nullif(split_part( tbl0_setup_collapse_id, '~',1 ),'THISISANULLVALplaceholdertoberemoved' ) as UNIQUE_ID_KEY, nullif(split_part( tbl0_setup_collapse_id, '~', 2), 'THISISANULLVALplaceholdertoberemoved' ) as STG_PERSON_HISTORY_MAR_STS_TYP_NM,  *
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
--********************************************************************************
-----------------------------setting up generic table tbl1_setup
---- rename to generic col names and select cols for history
tbl1_setup1 as (
select distinct 
		md5( coalesce(cast( CUST_ID as varchar ), '' ) ) as UNIQUE_ID_KEY,
    --data cols
    	
		MAILING_ADDRESS_CITY_NAME as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_CITY_NAME,
		MAILING_ADDRESS_COMMENT_TEXT as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COMMENT_TEXT,
		MAILING_ADDRESS_COUNTRY_NAME as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COUNTRY_NAME,
		MAILING_ADDRESS_COUNTY_NAME as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COUNTY_NAME,
		MAILING_ADDRESS_POSTAL_CODE as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_POSTAL_CODE,
		MAILING_ADDRESS_STATE_CODE as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_STATE_CODE,
		MAILING_ADDRESS_STATE_NAME as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_STATE_NAME,
		MAILING_ADDRESS_VALIDATED_IND as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_VALIDATED_IND,
		MAILING_FORMATTED_ADDRESS_POSTAL_CODE as STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_POSTAL_CODE,
		MAILING_FORMATTED_ADDRESS_ZIP4_CODE as STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_ZIP4_CODE,
		MAILING_FORMATTED_ADDRESS_ZIP_CODE as STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_ZIP_CODE,
		MAILING_STREET_ADDRESS_1 as STG_CUSTOMER_ADDRESS_MAIL_MAILING_STREET_ADDRESS_1,
		MAILING_STREET_ADDRESS_2 as STG_CUSTOMER_ADDRESS_MAIL_MAILING_STREET_ADDRESS_2
,

    --start end cols
    
        IFF( IFNULL( try_to_timestamp( MAIL_ADRS_EFF_DATE::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), MAIL_ADRS_EFF_DATE )
          as dbt_valid_from_initial_1,
    
        IFF( IFNULL( try_to_timestamp( MAIL_ADRS_END_DATE::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), MAIL_ADRS_END_DATE )
          as dbt_valid_to_1 

    from STAGING.STG_CUSTOMER_ADDRESS_MAIL
    
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
        select *, min( UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_STREET_ADDRESS_1 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_STREET_ADDRESS_2 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_STATE_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_STATE_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COUNTRY_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_CITY_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COUNTY_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_POSTAL_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_POSTAL_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_ZIP_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_ZIP4_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_VALIDATED_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COMMENT_TEXT as varchar ), 'THISISANULLVALplaceholdertoberemoved' ) ) OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_STREET_ADDRESS_1 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_STREET_ADDRESS_2 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_STATE_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_STATE_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COUNTRY_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_CITY_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COUNTY_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_POSTAL_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_POSTAL_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_ZIP_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_ZIP4_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_VALIDATED_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COMMENT_TEXT as varchar ), 'THISISANULLVALplaceholdertoberemoved' )) as tbl1_setup_collapse_id
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
        select     nullif(split_part( tbl1_setup_collapse_id, '~',1 ),'THISISANULLVALplaceholdertoberemoved' ) as UNIQUE_ID_KEY, nullif(split_part( tbl1_setup_collapse_id, '~', 2), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_MAIL_MAILING_STREET_ADDRESS_1, nullif(split_part( tbl1_setup_collapse_id, '~', 3), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_MAIL_MAILING_STREET_ADDRESS_2, nullif(split_part( tbl1_setup_collapse_id, '~', 4), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_STATE_CODE, nullif(split_part( tbl1_setup_collapse_id, '~', 5), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_STATE_NAME, nullif(split_part( tbl1_setup_collapse_id, '~', 6), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COUNTRY_NAME, nullif(split_part( tbl1_setup_collapse_id, '~', 7), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_CITY_NAME, nullif(split_part( tbl1_setup_collapse_id, '~', 8), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COUNTY_NAME, nullif(split_part( tbl1_setup_collapse_id, '~', 9), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_POSTAL_CODE, nullif(split_part( tbl1_setup_collapse_id, '~', 10), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_POSTAL_CODE, nullif(split_part( tbl1_setup_collapse_id, '~', 11), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_ZIP_CODE, nullif(split_part( tbl1_setup_collapse_id, '~', 12), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_ZIP4_CODE, nullif(split_part( tbl1_setup_collapse_id, '~', 13), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_VALIDATED_IND, nullif(split_part( tbl1_setup_collapse_id, '~', 14), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COMMENT_TEXT,  *
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
		md5( coalesce(cast( CUST_ID as varchar ), '' ) ) as UNIQUE_ID_KEY,
    --data cols
    	
		PHYSICAL_ADDRESS_CITY_NAME as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_CITY_NAME,
		PHYSICAL_ADDRESS_COMMENT_TEXT as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COMMENT_TEXT,
		PHYSICAL_ADDRESS_COUNTRY_NAME as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COUNTRY_NAME,
		PHYSICAL_ADDRESS_COUNTY_NAME as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COUNTY_NAME,
		PHYSICAL_ADDRESS_POSTAL_CODE as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_POSTAL_CODE,
		PHYSICAL_ADDRESS_STATE_CODE as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_STATE_CODE,
		PHYSICAL_ADDRESS_STATE_NAME as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_STATE_NAME,
		PHYSICAL_ADDRESS_VALIDATED_IND as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_VALIDATED_IND,
		PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE,
		PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE,
		PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE,
		PHYSICAL_STREET_ADDRESS_1 as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_STREET_ADDRESS_1,
		PHYSICAL_STREET_ADDRESS_2 as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_STREET_ADDRESS_2
,

    --start end cols
    
        IFF( IFNULL( try_to_timestamp( PHYS_ADRS_EFF_DATE::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), PHYS_ADRS_EFF_DATE )
          as dbt_valid_from_initial_1,
    
        IFF( IFNULL( try_to_timestamp( PHYS_ADRS_END_DATE::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), PHYS_ADRS_END_DATE )
          as dbt_valid_to_1 

    from STAGING.STG_CUSTOMER_ADDRESS_PHYS
    
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
        select *, min( UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_STREET_ADDRESS_1 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_STREET_ADDRESS_2 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_STATE_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_STATE_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COUNTRY_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_CITY_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COUNTY_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_POSTAL_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_VALIDATED_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COMMENT_TEXT as varchar ), 'THISISANULLVALplaceholdertoberemoved' ) ) OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_STREET_ADDRESS_1 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_STREET_ADDRESS_2 as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_STATE_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_STATE_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COUNTRY_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_CITY_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COUNTY_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_POSTAL_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_VALIDATED_IND as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COMMENT_TEXT as varchar ), 'THISISANULLVALplaceholdertoberemoved' )) as tbl2_setup_collapse_id
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
        select     nullif(split_part( tbl2_setup_collapse_id, '~',1 ),'THISISANULLVALplaceholdertoberemoved' ) as UNIQUE_ID_KEY, nullif(split_part( tbl2_setup_collapse_id, '~', 2), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_STREET_ADDRESS_1, nullif(split_part( tbl2_setup_collapse_id, '~', 3), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_STREET_ADDRESS_2, nullif(split_part( tbl2_setup_collapse_id, '~', 4), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_STATE_CODE, nullif(split_part( tbl2_setup_collapse_id, '~', 5), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_STATE_NAME, nullif(split_part( tbl2_setup_collapse_id, '~', 6), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COUNTRY_NAME, nullif(split_part( tbl2_setup_collapse_id, '~', 7), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_CITY_NAME, nullif(split_part( tbl2_setup_collapse_id, '~', 8), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COUNTY_NAME, nullif(split_part( tbl2_setup_collapse_id, '~', 9), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_POSTAL_CODE, nullif(split_part( tbl2_setup_collapse_id, '~', 10), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE, nullif(split_part( tbl2_setup_collapse_id, '~', 11), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE, nullif(split_part( tbl2_setup_collapse_id, '~', 12), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE, nullif(split_part( tbl2_setup_collapse_id, '~', 13), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_VALIDATED_IND, nullif(split_part( tbl2_setup_collapse_id, '~', 14), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COMMENT_TEXT,  *
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
    	
		CUST_NM_FST as STG_CUSTOMER_NAME_CUST_NM_FST,
		CUST_NM_LST as STG_CUSTOMER_NAME_CUST_NM_LST,
		CUST_NM_MID as STG_CUSTOMER_NAME_CUST_NM_MID,
		CUST_NM_NM as STG_CUSTOMER_NAME_CUST_NM_NM,
		CUST_NM_SFX_TYP_NM as STG_CUSTOMER_NAME_CUST_NM_SFX_TYP_NM,
		CUST_NM_TTL_TYP_NM as STG_CUSTOMER_NAME_CUST_NM_TTL_TYP_NM
,

    --start end cols
    
        IFF( IFNULL( try_to_timestamp( CUST_NM_EFF_DT::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), CUST_NM_EFF_DT )
          as dbt_valid_from_initial_1,
    
        IFF( IFNULL( try_to_timestamp( CUST_NM_END_DT::TEXT ), TO_TIMESTAMP( '2999-12-31 00:00:00' )) > CURRENT_DATE, TO_TIMESTAMP( '2999-12-31 00:00:00' ), CUST_NM_END_DT )
          as dbt_valid_to_1 

    from STAGING.STG_CUSTOMER_NAME
    WHERE CUST_NM_TYP_CD = 'PRSN_NM' AND VOID_IND = 'N'
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
        select *, min( UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_NAME_CUST_NM_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_NAME_CUST_NM_TTL_TYP_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_NAME_CUST_NM_FST as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_NAME_CUST_NM_MID as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_NAME_CUST_NM_LST as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_NAME_CUST_NM_SFX_TYP_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' ) ) OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_NAME_CUST_NM_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_NAME_CUST_NM_TTL_TYP_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_NAME_CUST_NM_FST as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_NAME_CUST_NM_MID as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_NAME_CUST_NM_LST as varchar ), 'THISISANULLVALplaceholdertoberemoved' )|| '~' ||coalesce(cast( STG_CUSTOMER_NAME_CUST_NM_SFX_TYP_NM as varchar ), 'THISISANULLVALplaceholdertoberemoved' )) as tbl3_setup_collapse_id
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
        select     nullif(split_part( tbl3_setup_collapse_id, '~',1 ),'THISISANULLVALplaceholdertoberemoved' ) as UNIQUE_ID_KEY, nullif(split_part( tbl3_setup_collapse_id, '~', 2), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_NAME_CUST_NM_NM, nullif(split_part( tbl3_setup_collapse_id, '~', 3), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_NAME_CUST_NM_TTL_TYP_NM, nullif(split_part( tbl3_setup_collapse_id, '~', 4), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_NAME_CUST_NM_FST, nullif(split_part( tbl3_setup_collapse_id, '~', 5), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_NAME_CUST_NM_MID, nullif(split_part( tbl3_setup_collapse_id, '~', 6), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_NAME_CUST_NM_LST, nullif(split_part( tbl3_setup_collapse_id, '~', 7), 'THISISANULLVALplaceholdertoberemoved' ) as STG_CUSTOMER_NAME_CUST_NM_SFX_TYP_NM,  *
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
-- Setting up generic table tbl4_setup
-- rename to generic col names and select cols for history
-- ********************************************************************************
tbl4_setup1 as (
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
    where BLK_TYP_CD in ('ALL_DOCM_BLK', 'CLM_DOCM_BLK', 'PLCY_DOCM_BLK') AND VOID_IND = 'N' 
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
        select *, min( UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_BLOCK_DOCUMENT_BLOCK_IND as varchar ), '☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺' ) ) 
		OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_BLOCK_DOCUMENT_BLOCK_IND as varchar ), '☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺' )) as tbl4_setup_collapse_id
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
        select     nullif(split_part( tbl4_setup_collapse_id, '~',1 ),'☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺' ) as UNIQUE_ID_KEY, nullif(split_part( tbl4_setup_collapse_id, '~', 2), '☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺' ) as DOCUMENT_BLOCK_IND,  *
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

    ,
--********************************************************************************
-- Setting up generic table tbl5_setup
-- rename to generic col names and select cols for history
-- ********************************************************************************
tbl5_setup1 as (
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
    where BLK_TYP_CD = 'ALERT' AND VOID_IND = 'N' 
        )
---- only select latest values for a day
, tbl5_setup2 as (
    select *, 
           ROW_NUMBER () OVER ( PARTITION BY UNIQUE_ID_KEY, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) ORDER BY DBT_VALID_FROM_INITIAL_1 DESC ) AS tbl5_ROWN 
    from     tbl5_setup1 
            qualify tbl5_ROWN = 1
        )
---- convert to a date, for comparisons across tables time differences do not matter, need to convert to ts at the end for dbt
, tbl5_setup as (
    select *, TO_DATE( DBT_VALID_FROM_INITIAL_1 ) as DBT_VALID_FROM_INITIAL, TO_DATE( dbt_valid_to_1 ) as dbt_valid_to
    from     tbl5_setup2 
        )        
        

--********************************************************************************----------------------------- creating collapse id of all tracked columns and making it unique to remove duplicate rows
    , tbl5_setup_build_granularity as (
        select *, min( UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_BLOCK_THREATENING_BEHAVIOR_BLOCK_IND as varchar ), '☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺' ) ) 
		OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast( STG_CUSTOMER_BLOCK_THREATENING_BEHAVIOR_BLOCK_IND as varchar ), '☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺' )) as tbl5_setup_collapse_id
    from tbl5_setup
    )



    , tbl5_setup_add_lag as ( --- first collapse
    SELECT
        *,
        ROW_NUMBER() OVER( PARTITION BY tbl5_setup_collapse_id ORDER BY dbt_valid_from_initial, dbt_valid_to) AS RN,
        LAG( dbt_valid_to, 1 ) OVER ( PARTITION BY tbl5_setup_collapse_id ORDER BY dbt_valid_from_initial, dbt_valid_to) AS PEND
    FROM
         tbl5_setup_build_granularity
    )
    --- mark all rows that have duplicate values for a range of dates based on the row number that starts the dates
--                                GAPID(starting row number)
--                                    SUM
-- Jan1,Jan 2 a  ->  Jan1,Jan2 a   23 23
-- Jan2,Jan 3 a  ->  Jan2,Jan3 a   0  23
-- Jan3,Jan 4 a  ->  Jan3,Jan4 a   0  23

    , tbl5_setup_add_gapid as (
    select 
            *,
            case when dbt_valid_from_initial > dateadd( day, 1, tbl5_setup_add_lag.PEND ) then RN else 0 END as gap,
            SUM( gap ) OVER ( PARTITION BY tbl5_setup_collapse_id ORDER BY tbl5_setup_add_lag.RN ) AS gapid

--- group on the gapid to remove duplicate rows and reset start and end dates in a criss-cross(upper left, to bottom right)
--- result above becomes: Jan1,Jan 4 a  


    from tbl5_setup_add_lag )
    --- collapse every row with same gapid into one row 
    , tbl5_setup_collapse as 

        ( 
        select 
        tbl5_setup_collapse_id, min( dbt_valid_from_initial ) as dbt_valid_from_initial, max( dbt_valid_to ) as dbt_valid_to
        from tbl5_setup_add_gapid
        group by tbl5_setup_collapse_id, gapid
         )
---- group by removes the columns, use split_part to get the values and columns back               
    , tbl5_setup_granularity as (
        select     nullif(split_part( tbl5_setup_collapse_id, '~',1 ),'☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺' ) as UNIQUE_ID_KEY, nullif(split_part( tbl5_setup_collapse_id, '~', 2), '☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺' ) as THREATENING_BEHAVIOR_BLOCK_IND,  *
        from tbl5_setup_collapse
    )



----------------------filling in any gaps in the dates
--  jan2   jan3      becomes jan2  jan3
--  jan6   jan7              jan3  jan7


, tbl5_add_lag2 as (
SELECT
    *,
    LAG( dbt_valid_to, 1 ) OVER( PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial, dbt_valid_to ) AS PREV_dbt_valid_to,
    ROW_NUMBER() OVER( PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial, dbt_valid_to ) AS RN --  for smushing
    FROM
        tbl5_setup_granularity
order by UNIQUE_ID_KEY, dbt_valid_from_initial
)
--- if jan6( next row initial ) > jan3( previous_to )
, tbl5_mark_gap2 as ( 
select 
        *,
--        case when  dbt_valid_from_initial > DATEADD( 'DAY', 1, PREV_dbt_valid_to )  then 1 else 0 END as gap,
--        case when  dbt_valid_from_initial =  PREV_dbt_valid_to  then DATEADD( 'DAY', 1, dbt_valid_from_initial ) else dbt_valid_from_initial END as dbt_valid_from_2a -- making 
        case when  dbt_valid_from_initial >  PREV_dbt_valid_to   then 1 else 0 END as gap,
        case when  dbt_valid_from_initial =  PREV_dbt_valid_to  then dbt_valid_from_initial  else dbt_valid_from_initial END as dbt_valid_from_2a -- placeholder for dateadd 
from tbl5_add_lag2

)
--- if there is a gap use the previous valid_to as the new valid_from
, tbl5_fill_gap as (
select *,
    --case when gap = 1 then  DATEADD( 'DAY', 1, PREV_dbt_valid_to ) else  dbt_valid_from_2a  end as dbt_valid_from_2b  -- made up history!!!
    case when gap = 1 then  PREV_dbt_valid_to else  dbt_valid_from_2a  end as dbt_valid_from_2b  -- made up history!!!
    from tbl5_mark_gap2
)
--- rewrite dbt_valid_from_2B to be the actual dbt_valid_from
, tbl5 as (
--    select  UNIQUE_ID_KEY, dbt_valid_from_2B as  dbt_valid_from, dbt_valid_to from  fill_gap
select *, dbt_valid_from_2B as  dbt_valid_from from  tbl5_fill_gap
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
		union
	select UNIQUE_ID_KEY, adate from tbl4 unpivot( adate for headers in ( dbt_valid_from, dbt_valid_to )) 
		union
	select UNIQUE_ID_KEY, adate from tbl5 unpivot( adate for headers in ( dbt_valid_from, dbt_valid_to )) 
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
	tbl0.STG_PERSON_HISTORY_MAR_STS_TYP_NM,
	tbl1.STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_CITY_NAME,
	tbl1.STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COMMENT_TEXT,
	tbl1.STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COUNTRY_NAME,
	tbl1.STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COUNTY_NAME,
	tbl1.STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_POSTAL_CODE,
	tbl1.STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_STATE_CODE,
	tbl1.STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_STATE_NAME,
	tbl1.STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_VALIDATED_IND,
	tbl1.STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_POSTAL_CODE,
	tbl1.STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_ZIP4_CODE,
	tbl1.STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_ZIP_CODE,
	tbl1.STG_CUSTOMER_ADDRESS_MAIL_MAILING_STREET_ADDRESS_1,
	tbl1.STG_CUSTOMER_ADDRESS_MAIL_MAILING_STREET_ADDRESS_2,
	tbl2.STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_CITY_NAME,
	tbl2.STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COMMENT_TEXT,
	tbl2.STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COUNTRY_NAME,
	tbl2.STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COUNTY_NAME,
	tbl2.STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_POSTAL_CODE,
	tbl2.STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_STATE_CODE,
	tbl2.STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_STATE_NAME,
	tbl2.STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_VALIDATED_IND,
	tbl2.STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE,
	tbl2.STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE,
	tbl2.STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE,
	tbl2.STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_STREET_ADDRESS_1,
	tbl2.STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_STREET_ADDRESS_2,
	tbl3.STG_CUSTOMER_NAME_CUST_NM_FST,
	tbl3.STG_CUSTOMER_NAME_CUST_NM_LST,
	tbl3.STG_CUSTOMER_NAME_CUST_NM_MID,
	tbl3.STG_CUSTOMER_NAME_CUST_NM_NM,
	tbl3.STG_CUSTOMER_NAME_CUST_NM_SFX_TYP_NM,
	tbl3.STG_CUSTOMER_NAME_CUST_NM_TTL_TYP_NM,
	tbl4.DOCUMENT_BLOCK_IND,
	tbl5.THREATENING_BEHAVIOR_BLOCK_IND,
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
        and  tbl3.dbt_valid_from < final_dates.dbt_valid_to  and  tbl3.dbt_valid_to > final_dates.dbt_valid_from
	left join tbl4 on  tbl4.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY 
        and  tbl4.dbt_valid_from < final_dates.dbt_valid_to  and  tbl4.dbt_valid_to > final_dates.dbt_valid_from
	left join tbl5 on  tbl5.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY 
        and  tbl5.dbt_valid_from < final_dates.dbt_valid_to  and  tbl5.dbt_valid_to > final_dates.dbt_valid_from		
		)
        ------------name back to std names
            , rename as (
                select distinct
                    UNIQUE_ID_KEY, --DEBUG_ID,
                    md5(coalesce(cast( UNIQUE_ID_KEY as varchar ), '') || '|' || coalesce(cast( CURRENT_TIMESTAMP as varchar ), '')) as dbt_scd_id,

                    --data cols
                    
		STG_PERSON_HISTORY_MAR_STS_TYP_NM as MAR_STS_TYP_NM ,
		STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_CITY_NAME as MAILING_ADDRESS_CITY_NAME ,
		STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COMMENT_TEXT as MAILING_ADDRESS_COMMENT_TEXT ,
		STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COUNTRY_NAME as MAILING_ADDRESS_COUNTRY_NAME ,
		STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_COUNTY_NAME as MAILING_ADDRESS_COUNTY_NAME ,
		STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_POSTAL_CODE as MAILING_ADDRESS_POSTAL_CODE ,
		STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_STATE_CODE as MAILING_ADDRESS_STATE_CODE ,
		STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_STATE_NAME as MAILING_ADDRESS_STATE_NAME ,
		STG_CUSTOMER_ADDRESS_MAIL_MAILING_ADDRESS_VALIDATED_IND as MAILING_ADDRESS_VALIDATED_IND ,
		STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_POSTAL_CODE as MAILING_FORMATTED_ADDRESS_POSTAL_CODE ,
		STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_ZIP4_CODE as MAILING_FORMATTED_ADDRESS_ZIP4_CODE ,
		STG_CUSTOMER_ADDRESS_MAIL_MAILING_FORMATTED_ADDRESS_ZIP_CODE as MAILING_FORMATTED_ADDRESS_ZIP_CODE ,
		STG_CUSTOMER_ADDRESS_MAIL_MAILING_STREET_ADDRESS_1 as MAILING_STREET_ADDRESS_1 ,
		STG_CUSTOMER_ADDRESS_MAIL_MAILING_STREET_ADDRESS_2 as MAILING_STREET_ADDRESS_2 ,
		STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_CITY_NAME as PHYSICAL_ADDRESS_CITY_NAME ,
		STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COMMENT_TEXT as PHYSICAL_ADDRESS_COMMENT_TEXT ,
		STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COUNTRY_NAME as PHYSICAL_ADDRESS_COUNTRY_NAME ,
		STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_COUNTY_NAME as PHYSICAL_ADDRESS_COUNTY_NAME ,
		STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_POSTAL_CODE as PHYSICAL_ADDRESS_POSTAL_CODE ,
		STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_STATE_CODE as PHYSICAL_ADDRESS_STATE_CODE ,
		STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_STATE_NAME as PHYSICAL_ADDRESS_STATE_NAME ,
		STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_ADDRESS_VALIDATED_IND as PHYSICAL_ADDRESS_VALIDATED_IND ,
		STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE as PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE ,
		STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE as PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE ,
		STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE as PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE ,
		STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_STREET_ADDRESS_1 as PHYSICAL_STREET_ADDRESS_1 ,
		STG_CUSTOMER_ADDRESS_PHYS_PHYSICAL_STREET_ADDRESS_2 as PHYSICAL_STREET_ADDRESS_2 ,
		STG_CUSTOMER_NAME_CUST_NM_FST as CUST_NM_FST ,
		STG_CUSTOMER_NAME_CUST_NM_LST as CUST_NM_LST ,
		STG_CUSTOMER_NAME_CUST_NM_MID as CUST_NM_MID ,
		STG_CUSTOMER_NAME_CUST_NM_NM as CUST_NM_NM ,
		STG_CUSTOMER_NAME_CUST_NM_SFX_TYP_NM as CUST_NM_SFX_TYP_NM ,
		STG_CUSTOMER_NAME_CUST_NM_TTL_TYP_NM as CUST_NM_TTL_TYP_NM ,
		DOCUMENT_BLOCK_IND as DOCUMENT_BLOCK_IND,
	    THREATENING_BEHAVIOR_BLOCK_IND as THREATENING_BEHAVIOR_BLOCK_IND,

                    to_timestamp_ntz( current_timestamp ) as dbt_updated_at,
                    --start end
                    dbt_valid_from,
                    NULLIF( dbt_valid_to, TO_DATE('12/31/2999') ) as dbt_valid_to
                    from join_sql
            )
    

--********************************************************************************----------------------------- CREATING COLLAPSE ID OF ALL TRACKED COLUMNS AND MAKING IT UNIQUE TO REMOVE DUPLICATE ROWS
    , RENAME_BUILD_GRANULARITY AS (
        SELECT *, 
		MIN( UNIQUE_ID_KEY|| '~' ||COALESCE(CAST( MAR_STS_TYP_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_STREET_ADDRESS_1 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_STREET_ADDRESS_2 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_STATE_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_STATE_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_COUNTRY_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_CITY_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_COUNTY_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_POSTAL_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_FORMATTED_ADDRESS_POSTAL_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_FORMATTED_ADDRESS_ZIP_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_FORMATTED_ADDRESS_ZIP4_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_VALIDATED_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_COMMENT_TEXT AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_STREET_ADDRESS_1 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_STREET_ADDRESS_2 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_STATE_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_STATE_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_COUNTRY_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_CITY_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_COUNTY_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_POSTAL_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_VALIDATED_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_COMMENT_TEXT AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( CUST_NM_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( CUST_NM_TTL_TYP_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( CUST_NM_FST AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( CUST_NM_MID AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( CUST_NM_LST AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( CUST_NM_SFX_TYP_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( DOCUMENT_BLOCK_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) || '~' ||
		COALESCE(CAST( THREATENING_BEHAVIOR_BLOCK_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )  
		) 
		OVER( PARTITION BY UNIQUE_ID_KEY|| '~' ||
		COALESCE(CAST( MAR_STS_TYP_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_STREET_ADDRESS_1 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_STREET_ADDRESS_2 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_STATE_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_STATE_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_COUNTRY_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_CITY_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_COUNTY_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_POSTAL_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_FORMATTED_ADDRESS_POSTAL_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_FORMATTED_ADDRESS_ZIP_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_FORMATTED_ADDRESS_ZIP4_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_VALIDATED_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( MAILING_ADDRESS_COMMENT_TEXT AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_STREET_ADDRESS_1 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_STREET_ADDRESS_2 AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_STATE_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_STATE_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_COUNTRY_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_CITY_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_COUNTY_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_POSTAL_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_VALIDATED_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( PHYSICAL_ADDRESS_COMMENT_TEXT AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( CUST_NM_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( CUST_NM_TTL_TYP_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( CUST_NM_FST AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( CUST_NM_MID AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( CUST_NM_LST AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( CUST_NM_SFX_TYP_NM AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )|| '~' ||
		COALESCE(CAST( DOCUMENT_BLOCK_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) || '~' ||
		COALESCE(CAST( THREATENING_BEHAVIOR_BLOCK_IND AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' )  
		) 
		AS RENAME_COLLAPSE_ID
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
      CONDITIONAL_CHANGE_EVENT(RENAME_COLLAPSE_ID) OVER (PARTITION BY UNIQUE_ID_KEY ORDER BY DBT_VALID_FROM, DBT_VALID_TO) AS CCE
      , CASE WHEN DBT_VALID_FROM = LEAD(DBT_VALID_FROM) OVER (PARTITION BY UNIQUE_ID_KEY ORDER BY DBT_VALID_FROM, DBT_VALID_TO)
                    THEN 'Y' ELSE 'N' END AS FLG

--- GROUP ON THE GAPID TO REMOVE DUPLICATE ROWS AND RESET START AND END DATES IN A CRISS-CROSS(UPPER LEFT, TO BOTTOM RIGHT)
--- RESULT ABOVE BECOMES: JAN1,JAN 4 A  


    FROM RENAME_ADD_LAG )
    --- COLLAPSE EVERY ROW WITH SAME GAPID INTO ONE ROW 
    , RENAME_COLLAPSE AS 

        ( 
        SELECT 
        RENAME_COLLAPSE_ID, MIN( DBT_VALID_FROM ) AS DBT_VALID_FROM, MAX( DBT_VALID_TO ) AS DBT_VALID_TO
        FROM RENAME_ADD_GAPID
        WHERE FLG ='N'
        GROUP BY RENAME_COLLAPSE_ID, GAPID, CCE
        )
---- GROUP BY REMOVES THE COLUMNS, USE SPLIT_PART TO GET THE VALUES AND COLUMNS BACK               
    , RENAME_GRANULARITY AS (
        SELECT     NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~',1 ),'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS UNIQUE_ID_KEY, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 2), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAR_STS_TYP_NM, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 3), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_STREET_ADDRESS_1, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 4), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_STREET_ADDRESS_2, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 5), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_ADDRESS_STATE_CODE, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 6), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_ADDRESS_STATE_NAME, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 7), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_ADDRESS_COUNTRY_NAME, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 8), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_ADDRESS_CITY_NAME, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 9), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_ADDRESS_COUNTY_NAME, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 10), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_ADDRESS_POSTAL_CODE, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 11), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_FORMATTED_ADDRESS_POSTAL_CODE, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 12), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_FORMATTED_ADDRESS_ZIP_CODE, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 13), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_FORMATTED_ADDRESS_ZIP4_CODE, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 14), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_ADDRESS_VALIDATED_IND, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 15), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS MAILING_ADDRESS_COMMENT_TEXT, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 16), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_STREET_ADDRESS_1, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 17), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_STREET_ADDRESS_2, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 18), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_ADDRESS_STATE_CODE, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 19), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_ADDRESS_STATE_NAME, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 20), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_ADDRESS_COUNTRY_NAME, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 21), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_ADDRESS_CITY_NAME, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 22), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_ADDRESS_COUNTY_NAME, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 23), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_ADDRESS_POSTAL_CODE, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 24), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 25), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 26), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 27), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_ADDRESS_VALIDATED_IND, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 28), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS PHYSICAL_ADDRESS_COMMENT_TEXT, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 29), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CUST_NM_NM, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 30), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CUST_NM_TTL_TYP_NM, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 31), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CUST_NM_FST, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 32), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CUST_NM_MID, NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 33), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CUST_NM_LST, 
        NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 34), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS CUST_NM_SFX_TYP_NM,  
        NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 35), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS DOCUMENT_BLOCK_IND,
      NULLIF(SPLIT_PART( RENAME_COLLAPSE_ID, '~', 36), 'THISISANULLVALPLACEHOLDERTOBEREMOVED' ) AS THREATENING_BEHAVIOR_BLOCK_IND, *
        
        FROM RENAME_COLLAPSE
    )



, final_sql as ( select 
	UNIQUE_ID_KEY
	, md5(coalesce(cast( UNIQUE_ID_KEY as varchar ), '') || '|' || coalesce(cast( CURRENT_TIMESTAMP as varchar ), '')) as dbt_scd_id
	, to_timestamp( DBT_VALID_FROM ) as DBT_VALID_FROM
	, to_timestamp( case when  ( row_number() over ( partition by UNIQUE_ID_KEY order by DBT_VALID_FROM DESC, DBT_VALID_TO DESC)) =1 THEN NULL 
                            -- WHEN (UNIQUE_ID_KEY = LEAD(UNIQUE_ID_KEY) OVER (ORDER BY UNIQUE_ID_KEY, DBT_VALID_FROM, DBT_VALID_TO)
                                    -- AND DBT_VALID_TO > LEAD(DBT_VALID_FROM) OVER (partition by UNIQUE_ID_KEY order by DBT_VALID_FROM , DBT_VALID_TO ))
                            -- THEN LEAD(DBT_VALID_FROM) OVER (partition by UNIQUE_ID_KEY order by DBT_VALID_FROM , DBT_VALID_TO )
                            else  DBT_VALID_TO end )  AS DBT_VALID_TO
	, to_timestamp( current_timestamp ) as DBT_UPDATED_AT
	, MAR_STS_TYP_NM AS MARITAL_STATUS_TYPE_DESC
	, MAILING_STREET_ADDRESS_1
	, MAILING_STREET_ADDRESS_2
	, MAILING_ADDRESS_STATE_CODE
	, MAILING_ADDRESS_STATE_NAME
	, MAILING_ADDRESS_COUNTRY_NAME
	, MAILING_ADDRESS_CITY_NAME
	, MAILING_ADDRESS_COUNTY_NAME
	, MAILING_ADDRESS_POSTAL_CODE
	, MAILING_FORMATTED_ADDRESS_POSTAL_CODE
	, MAILING_FORMATTED_ADDRESS_ZIP_CODE
	, MAILING_FORMATTED_ADDRESS_ZIP4_CODE
	, MAILING_ADDRESS_VALIDATED_IND
	, MAILING_ADDRESS_COMMENT_TEXT
	, PHYSICAL_STREET_ADDRESS_1
	, PHYSICAL_STREET_ADDRESS_2
	, PHYSICAL_ADDRESS_STATE_CODE
	, PHYSICAL_ADDRESS_STATE_NAME
	, PHYSICAL_ADDRESS_COUNTRY_NAME
	, PHYSICAL_ADDRESS_CITY_NAME
	, PHYSICAL_ADDRESS_COUNTY_NAME
	, PHYSICAL_ADDRESS_POSTAL_CODE
	, PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE
	, PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE
	, PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE
	, PHYSICAL_ADDRESS_VALIDATED_IND
	, PHYSICAL_ADDRESS_COMMENT_TEXT
	, CUST_NM_NM AS FULL_NAME
	, CUST_NM_TTL_TYP_NM AS COURTESY_TITLE_NAME
	, CUST_NM_FST AS FIRST_NAME
	, CUST_NM_MID AS MIDDLE_NAME
	, CUST_NM_LST AS LAST_NAME
	, CUST_NM_SFX_TYP_NM AS SUFFIX_NAME
    , NVL(DOCUMENT_BLOCK_IND,'N')AS DOCUMENT_BLOCK_IND 
    , NVL(THREATENING_BEHAVIOR_BLOCK_IND,'N') as THREATENING_BEHAVIOR_BLOCK_IND	
from rename_granularity  order by dbt_valid_from, dbt_valid_to )
    

--------PHASE 1: DEBUG
-- set Debug to True to test these

select *  from final_sql