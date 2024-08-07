---------PHASE 2 create table
--CREATE OR REPLACE table dev_edw.history.dim_network_snaphot_step1 as 
with 


--********************************************************************************
-----------------------------setting up generic table tbl0_setup
---- rename to generic col names and select cols for history
tbl0_setup1 as (
select distinct 
		md5(coalesce(cast(NTWK_NUMBER as varchar ), '')) as UNIQUE_ID_KEY,
    --data cols
    	
		END_DATE::DATE as STG_NETWORK_END_DATE,
		case when NTWK_NUMBER in ('20001','20003') then 'OHIO BUREAU OF WORKERS'' COMPENSATION' else NTWK_NAME end as STG_NETWORK_NTWK_NAME,
		NTWK_STATUS_DSC as STG_NETWORK_NTWK_STATUS_DSC,
		NTWK_STATUS_IDN as STG_NETWORK_NTWK_STATUS_IDN,
		START_DATE::DATE as STG_NETWORK_START_DATE
,

    --start end cols
    
        IFF(IFNULL(try_to_timestamp(NVL(START_DATE, '1901-01-01')::TEXT),TO_TIMESTAMP('1901-01-01 00:00:00'))>CURRENT_DATE,TO_TIMESTAMP('1901-01-01 00:00:00'),NVL(START_DATE, '1901-01-01'))
         as dbt_valid_from_initial_1,
    
        IFF(IFNULL(try_to_timestamp(END_DATE::TEXT),TO_TIMESTAMP('2999-12-31 00:00:00'))>CURRENT_DATE,TO_TIMESTAMP('2999-12-31 00:00:00'),END_DATE)
          as dbt_valid_to_1 

from STAGING.STG_NETWORK
WHERE NTWK_TYPE != 'QHP'
 QUALIFY (ROW_NUMBER() OVER(PARTITION BY NTWK_NUMBER,START_DATE ORDER BY START_DATE DESC, NVL(END_DATE, CURRENT_DATE) DESC )) =1
        )
---- only select latest values for a day
, tbl0_setup2 as (
    select *, 
           ROW_NUMBER () OVER (PARTITION BY UNIQUE_ID_KEY,TO_DATE(DBT_VALID_FROM_INITIAL_1) ORDER BY DBT_VALID_FROM_INITIAL_1 DESC) AS tbl0_ROWN 
    from     tbl0_setup1 
            qualify tbl0_ROWN=1
        )
---- convert to a date, for comparisons across tables time differences do not matter, need to convert to ts at the end for dbt
, tbl0_setup as (
    select *, TO_DATE(DBT_VALID_FROM_INITIAL_1) as DBT_VALID_FROM_INITIAL,TO_DATE(dbt_valid_to_1) as dbt_valid_to
    from     tbl0_setup2 
        )        
        

--********************************************************************************----------------------------- creating collapse id of all tracked columns and making it unique to remove duplicate rows
    , tbl0_setup_build_granularity as (
        select *,min(UNIQUE_ID_KEY|| '~' ||coalesce(cast(STG_NETWORK_NTWK_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_NETWORK_NTWK_STATUS_IDN as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_NETWORK_NTWK_STATUS_DSC as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_NETWORK_START_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_NETWORK_END_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved')) OVER(PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast(STG_NETWORK_NTWK_NAME as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_NETWORK_NTWK_STATUS_IDN as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_NETWORK_NTWK_STATUS_DSC as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_NETWORK_START_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_NETWORK_END_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved')) as tbl0_setup_collapse_id
    from tbl0_setup
    )



    , tbl0_setup_add_lag as ( --- first collapse
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY tbl0_setup_collapse_id ORDER BY dbt_valid_from_initial,dbt_valid_to) AS RN,
        LAG(dbt_valid_to,1) OVER (PARTITION BY tbl0_setup_collapse_id ORDER BY dbt_valid_from_initial, dbt_valid_to) AS PEND
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
            case when dbt_valid_from_initial > dateadd(day,1,tbl0_setup_add_lag.PEND) then RN else 0 END as gap,
            SUM(gap) OVER (PARTITION BY tbl0_setup_collapse_id ORDER BY tbl0_setup_add_lag.RN) AS gapid

--- group on the gapid to remove duplicate rows and reset start and end dates in a criss-cross(upper left, to bottom right)
--- result above becomes: Jan1,Jan 4 a  


    from tbl0_setup_add_lag )
    --- collapse every row with same gapid into one row 
    , tbl0_setup_collapse as 

        ( 
        select 
        tbl0_setup_collapse_id,min(dbt_valid_from_initial) as dbt_valid_from_initial,max(dbt_valid_to) as dbt_valid_to
        from tbl0_setup_add_gapid
        group by tbl0_setup_collapse_id,gapid
         )
---- group by removes the columns, use split_part to get the values and columns back               
    , tbl0_setup_granularity as (
        select     nullif(split_part(tbl0_setup_collapse_id,'~',1),'THISISANULLVALplaceholdertoberemoved') as UNIQUE_ID_KEY, nullif(split_part(tbl0_setup_collapse_id,'~',2),'THISISANULLVALplaceholdertoberemoved') as STG_NETWORK_NTWK_NAME, nullif(split_part(tbl0_setup_collapse_id,'~',3),'THISISANULLVALplaceholdertoberemoved') as STG_NETWORK_NTWK_STATUS_IDN, nullif(split_part(tbl0_setup_collapse_id,'~',4),'THISISANULLVALplaceholdertoberemoved') as STG_NETWORK_NTWK_STATUS_DSC, nullif(split_part(tbl0_setup_collapse_id,'~',5),'THISISANULLVALplaceholdertoberemoved') as STG_NETWORK_START_DATE, nullif(split_part(tbl0_setup_collapse_id,'~',6),'THISISANULLVALplaceholdertoberemoved') as STG_NETWORK_END_DATE,  *
        from tbl0_setup_collapse
    )



----------------------filling in any gaps in the dates
--  jan2   jan3      becomes jan2  jan3
--  jan6   jan7              jan3  jan7


,tbl0_add_lag2 as (
SELECT
    *,
    LAG(dbt_valid_to,1) OVER(PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial,dbt_valid_to) AS PREV_dbt_valid_to,
    ROW_NUMBER() OVER(PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial,dbt_valid_to) AS RN --  for smushing
    FROM
        tbl0_setup_granularity
order by UNIQUE_ID_KEY, dbt_valid_from_initial
)
--- if jan6(next row initial) > jan3(previous_to)
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
,tbl0 as (
--    select  UNIQUE_ID_KEY, dbt_valid_from_2B as  dbt_valid_from, dbt_valid_to from  fill_gap
select *,dbt_valid_from_2B as  dbt_valid_from from  tbl0_fill_gap
)

    
--********************************************************************************
-----------------------------FINAL DATES TABLE union all dates, join to get values
--	unpivot dbt_valid_from,dbt_valid_to into a single column adate, all other columns are gone
, combo_dates as (
    select UNIQUE_ID_KEY, adate from tbl0 unpivot(adate for headers in (dbt_valid_from,dbt_valid_to)) 
)

--- remove duplicate dates with distinct
, unique_dates as (
    select distinct * from combo_dates order by adate    
)
--- convert single column back into 2 columns aka pivot
, final_dates as (
    select UNIQUE_ID_KEY, 
        adate as dbt_valid_from, 
        lead(adate,1) over (partition by  UNIQUE_ID_KEY order by adate) dbt_valid_to 
from unique_dates  
)
    
----------------------JOIN ALL tables together with matching dates
, join_sql as ( 
select 
	tbl0.UNIQUE_ID_KEY,
	tbl0.STG_NETWORK_END_DATE,tbl0.STG_NETWORK_NTWK_NAME,tbl0.STG_NETWORK_NTWK_STATUS_DSC,tbl0.STG_NETWORK_NTWK_STATUS_IDN,tbl0.STG_NETWORK_START_DATE,
	final_dates.dbt_valid_from,final_dates.dbt_valid_to
    from tbl0
        inner join final_dates
        on tbl0.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY and tbl0.dbt_valid_from<final_dates.dbt_valid_to and tbl0.dbt_valid_to>final_dates.dbt_valid_from
            )
        ------------name back to std names
            , rename as (
                select distinct
                    UNIQUE_ID_KEY, --DEBUG_ID,
                    md5(coalesce(cast(UNIQUE_ID_KEY as varchar ), '') || '|' || coalesce(cast(CURRENT_TIMESTAMP as varchar ), '')) as dbt_scd_id,

                    --data cols
                    
		STG_NETWORK_END_DATE as END_DATE ,
		STG_NETWORK_NTWK_NAME as NTWK_NAME ,
		STG_NETWORK_NTWK_STATUS_DSC as NTWK_STATUS_DSC ,
		STG_NETWORK_NTWK_STATUS_IDN as NTWK_STATUS_IDN ,
		STG_NETWORK_START_DATE as START_DATE ,

                    to_timestamp_ntz(current_timestamp) as dbt_updated_at,
                    --start end
                    dbt_valid_from,
                    NULLIF(dbt_valid_to,TO_DATE('12/31/2999')) as dbt_valid_to
                    from join_sql
            )
    

--********************************************************************************----------------------------- CREATING COLLAPSE ID OF ALL TRACKED COLUMNS AND MAKING IT UNIQUE TO REMOVE DUPLICATE ROWS
    , RENAME_BUILD_GRANULARITY AS (
        SELECT *,MIN(UNIQUE_ID_KEY|| '~' ||COALESCE(CAST(NTWK_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(NTWK_STATUS_IDN AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(NTWK_STATUS_DSC AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(START_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(END_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')) OVER(PARTITION BY UNIQUE_ID_KEY|| '~' ||COALESCE(CAST(NTWK_NAME AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(NTWK_STATUS_IDN AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(NTWK_STATUS_DSC AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(START_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(END_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')) AS RENAME_COLLAPSE_ID
    FROM RENAME
    )



    , RENAME_ADD_LAG AS ( --- FIRST COLLAPSE
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY RENAME_COLLAPSE_ID ORDER BY DBT_VALID_FROM,DBT_VALID_TO) AS RN,
        LAG(DBT_VALID_TO,1) OVER (PARTITION BY RENAME_COLLAPSE_ID ORDER BY DBT_VALID_FROM, DBT_VALID_TO) AS PEND
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
            CASE WHEN DBT_VALID_FROM > DATEADD(DAY,1,RENAME_ADD_LAG.PEND) THEN RN ELSE 0 END AS GAP,
            SUM(GAP) OVER (PARTITION BY RENAME_COLLAPSE_ID ORDER BY RENAME_ADD_LAG.RN) AS GAPID

--- GROUP ON THE GAPID TO REMOVE DUPLICATE ROWS AND RESET START AND END DATES IN A CRISS-CROSS(UPPER LEFT, TO BOTTOM RIGHT)
--- RESULT ABOVE BECOMES: JAN1,JAN 4 A  


    FROM RENAME_ADD_LAG )
    --- COLLAPSE EVERY ROW WITH SAME GAPID INTO ONE ROW 
    , RENAME_COLLAPSE AS 

        ( 
        SELECT 
        RENAME_COLLAPSE_ID,MIN(DBT_VALID_FROM) AS DBT_VALID_FROM,MAX(DBT_VALID_TO) AS DBT_VALID_TO
        FROM RENAME_ADD_GAPID
        GROUP BY RENAME_COLLAPSE_ID,GAPID
         )
---- GROUP BY REMOVES THE COLUMNS, USE SPLIT_PART TO GET THE VALUES AND COLUMNS BACK               
    , RENAME_GRANULARITY AS (
        SELECT     NULLIF(SPLIT_PART(RENAME_COLLAPSE_ID,'~',1),'THISISANULLVALPLACEHOLDERTOBEREMOVED') AS UNIQUE_ID_KEY, NULLIF(SPLIT_PART(RENAME_COLLAPSE_ID,'~',2),'THISISANULLVALPLACEHOLDERTOBEREMOVED') AS NTWK_NAME, NULLIF(SPLIT_PART(RENAME_COLLAPSE_ID,'~',3),'THISISANULLVALPLACEHOLDERTOBEREMOVED') AS NTWK_STATUS_IDN, NULLIF(SPLIT_PART(RENAME_COLLAPSE_ID,'~',4),'THISISANULLVALPLACEHOLDERTOBEREMOVED') AS NTWK_STATUS_DSC, NULLIF(SPLIT_PART(RENAME_COLLAPSE_ID,'~',5),'THISISANULLVALPLACEHOLDERTOBEREMOVED') AS START_DATE, NULLIF(SPLIT_PART(RENAME_COLLAPSE_ID,'~',6),'THISISANULLVALPLACEHOLDERTOBEREMOVED') AS END_DATE,  *
        FROM RENAME_COLLAPSE
    )

, final_sql as ( select 
	UNIQUE_ID_KEY,md5(coalesce(cast(UNIQUE_ID_KEY as varchar ), '') || '|' || coalesce(cast(CURRENT_TIMESTAMP as varchar ), '')) as dbt_scd_id
    ,to_timestamp(DBT_VALID_FROM) as DBT_VALID_FROM,to_timestamp(DBT_VALID_TO) as DBT_VALID_TO,to_timestamp(current_timestamp) as DBT_UPDATED_AT
    ,NTWK_NAME AS NETWORK_NAME, NTWK_STATUS_IDN AS NETWORK_CERTIFICATION_STATUS_CODE, NTWK_STATUS_DSC AS NETWORK_CERTIFICATION_STATUS_DESC
    ,START_DATE as NETWORK_CERTIFICATION_EFFECTIVE_DATE, END_DATE as NETWORK_CERTIFICATION_END_DATE
from rename_granularity  order by dbt_valid_from,dbt_valid_to )
    

--------PHASE 1: DEBUG
-- set Debug to True to test these

select *  from final_sql   -- verify basically works

-- select UNIQUE_ID_KEY,count(*) as count from final_sql group by UNIQUE_ID_KEY order by count desc;  --find example keys

-- select *  from final_sql where DEBUG_ID like '%DA_KEY%' order by dbt_valid_from,dbt_valid_to ;
-- select *  from tbl0_setup where UNIQUE_KEY_ID like '%DA_KEY%' order by dbt_valid_from_initial,dbt_valid_to ;
-- select *  from tbl1_setup where UNIQUE_KEY_ID like '%DA_KEY%' order by dbt_valid_from_initial,dbt_valid_to ;
-- select *  from tbl2_setup where UNIQUE_KEY_ID like '%DA_KEY%' order by dbt_valid_from_initial,dbt_valid_to ;
-- select *  from tbl3_setup where UNIQUE_KEY_ID like '%DA_KEY%' order by dbt_valid_from_initial,dbt_valid_to ;
-- select *  from tbl4_setup where UNIQUE_KEY_ID like '%DA_KEY%' order by dbt_valid_from_initial,dbt_valid_to ;


---------PHASE 2: select for whole table 
-- select * from final_sql;
        
----------PHASE 3: CLONE to FINAL LOCATION 

    -------- check data types
    -- Example of the model:
    --create or replace TABLE DIM_ICD_SNAPSHOT_STEP1 (
    --           CMS_ICD_STATUS_CODE VARCHAR(16777216),
    --            CMS_ICD_STATUS_DESC VARCHAR(16777216),
    --            UNIQUE_ID_KEY VARCHAR(32),
    --            DBT_SCD_ID VARCHAR(32),
    --            DBT_UPDATED_AT TIMESTAMP_NTZ(9),
    --            DBT_VALID_FROM TIMESTAMP_NTZ(9),
    --            DBT_VALID_TO TIMESTAMP_NTZ(9)
    --);

    --create or replace table TGT_SCHEMA.dim_network_snaphot_step1 clone history.dim_network_snaphot_step1";