---------PHASE 2 create table
--CREATE OR REPLACE table dev_edw.history.dim_icd_snaphot_step1 as 
with 


--********************************************************************************
-----------------------------setting up generic table tbl0_setup
---- rename to generic col names and select cols for history
tbl0_setup1 as (
select distinct 
		md5(coalesce(cast(ICD_CODE as varchar ), '')|| '-' || coalesce(cast(ICD_CODE_VERSION_NUMBER as varchar ), '')) as UNIQUE_ID_KEY,
    --data cols
    	
		ICD_CODE_STATUS_CODE as STG_TDDIDCS_ICD_CODE_STATUS_CODE,
		ICD_CODE_STATUS_DESC as STG_TDDIDCS_ICD_CODE_STATUS_DESC,
		ICD_CODE_STATUS_EFFECTIVE_DATE as STG_TDDIDCS_ICD_CODE_STATUS_EFFECTIVE_DATE,
		ICD_CODE_STATUS_END_DATE as STG_TDDIDCS_ICD_CODE_STATUS_END_DATE
,

    --start end cols
    
        IFF(IFNULL(try_to_timestamp(ICD_CODE_STATUS_EFFECTIVE_DATE::TEXT),TO_TIMESTAMP('2999-12-31 00:00:00'))>CURRENT_DATE,TO_TIMESTAMP('2999-12-31 00:00:00'),ICD_CODE_STATUS_EFFECTIVE_DATE)
         as dbt_valid_from_initial_1,
    
        IFF(IFNULL(try_to_timestamp(ICD_CODE_STATUS_END_DATE::TEXT),TO_TIMESTAMP('2999-12-31 00:00:00'))>CURRENT_DATE,TO_TIMESTAMP('2999-12-31 00:00:00'),ICD_CODE_STATUS_END_DATE)
          as dbt_valid_to_1 

from STAGING.STG_TDDIDCS
WHERE IDCS_VRSN_END_DATE > CURRENT_DATE()
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
        select *,min(UNIQUE_ID_KEY|| '~' ||coalesce(cast(STG_TDDIDCS_ICD_CODE_STATUS_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_TDDIDCS_ICD_CODE_STATUS_DESC as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_TDDIDCS_ICD_CODE_STATUS_EFFECTIVE_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_TDDIDCS_ICD_CODE_STATUS_END_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved')) OVER(PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast(STG_TDDIDCS_ICD_CODE_STATUS_CODE as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_TDDIDCS_ICD_CODE_STATUS_DESC as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_TDDIDCS_ICD_CODE_STATUS_EFFECTIVE_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_TDDIDCS_ICD_CODE_STATUS_END_DATE as varchar ), 'THISISANULLVALplaceholdertoberemoved')) as tbl0_setup_collapse_id
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
        select     nullif(split_part(tbl0_setup_collapse_id,'~',1),'THISISANULLVALplaceholdertoberemoved') as UNIQUE_ID_KEY, nullif(split_part(tbl0_setup_collapse_id,'~',2),'THISISANULLVALplaceholdertoberemoved') as STG_TDDIDCS_ICD_CODE_STATUS_CODE, nullif(split_part(tbl0_setup_collapse_id,'~',3),'THISISANULLVALplaceholdertoberemoved') as STG_TDDIDCS_ICD_CODE_STATUS_DESC, nullif(split_part(tbl0_setup_collapse_id,'~',4),'THISISANULLVALplaceholdertoberemoved') as STG_TDDIDCS_ICD_CODE_STATUS_EFFECTIVE_DATE, nullif(split_part(tbl0_setup_collapse_id,'~',5),'THISISANULLVALplaceholdertoberemoved') as STG_TDDIDCS_ICD_CODE_STATUS_END_DATE,  *
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

    ,
--********************************************************************************
-----------------------------setting up generic table tbl1_setup
---- rename to generic col names and select cols for history
tbl1_setup1 as (
select distinct 
		md5(coalesce(cast(ICD_CODE as varchar ), '')|| '-' || coalesce(cast(ICD_CODE_VERSION_NUMBER as varchar ), '')) as UNIQUE_ID_KEY,
    --data cols
    	
		ICD_CODE_LONG_DESC as STG_TDDICDN_ICD_CODE_LONG_DESC,
		ICD_CODE_SHORT_DESC as STG_TDDICDN_ICD_CODE_SHORT_DESC
,

    --start end cols
    
        IFF(IFNULL(try_to_timestamp(ICDN_EFCTV_DATE::TEXT),TO_TIMESTAMP('2999-12-31 00:00:00'))>CURRENT_DATE,TO_TIMESTAMP('2999-12-31 00:00:00'),ICDN_EFCTV_DATE)
         as dbt_valid_from_initial_1,
    
        IFF(IFNULL(try_to_timestamp(ICDN_ENDNG_DATE::TEXT),TO_TIMESTAMP('2999-12-31 00:00:00'))>CURRENT_DATE,TO_TIMESTAMP('2999-12-31 00:00:00'),ICDN_ENDNG_DATE)
          as dbt_valid_to_1 

from DEV_EDW.STAGING.STG_TDDICDN
where icdn_vrsn_end_date  > CURRENT_DATE()
        )
---- only select latest values for a day
, tbl1_setup2 as (
    select *, 
           ROW_NUMBER () OVER (PARTITION BY UNIQUE_ID_KEY,TO_DATE(DBT_VALID_FROM_INITIAL_1) ORDER BY DBT_VALID_FROM_INITIAL_1 DESC) AS tbl1_ROWN 
    from     tbl1_setup1 
            qualify tbl1_ROWN=1
        )
---- convert to a date, for comparisons across tables time differences do not matter, need to convert to ts at the end for dbt
, tbl1_setup as (
    select *, TO_DATE(DBT_VALID_FROM_INITIAL_1) as DBT_VALID_FROM_INITIAL,TO_DATE(dbt_valid_to_1) as dbt_valid_to
    from     tbl1_setup2 
        )        
        

--********************************************************************************----------------------------- creating collapse id of all tracked columns and making it unique to remove duplicate rows
    , tbl1_setup_build_granularity as (
        select *,min(UNIQUE_ID_KEY|| '~' ||coalesce(cast(STG_TDDICDN_ICD_CODE_SHORT_DESC as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_TDDICDN_ICD_CODE_LONG_DESC as varchar ), 'THISISANULLVALplaceholdertoberemoved')) OVER(PARTITION BY UNIQUE_ID_KEY|| '~' ||coalesce(cast(STG_TDDICDN_ICD_CODE_SHORT_DESC as varchar ), 'THISISANULLVALplaceholdertoberemoved')|| '~' ||coalesce(cast(STG_TDDICDN_ICD_CODE_LONG_DESC as varchar ), 'THISISANULLVALplaceholdertoberemoved')) as tbl1_setup_collapse_id
    from tbl1_setup
    )



    , tbl1_setup_add_lag as ( --- first collapse
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY tbl1_setup_collapse_id ORDER BY dbt_valid_from_initial,dbt_valid_to) AS RN,
        LAG(dbt_valid_to,1) OVER (PARTITION BY tbl1_setup_collapse_id ORDER BY dbt_valid_from_initial, dbt_valid_to) AS PEND
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
            case when dbt_valid_from_initial > dateadd(day,1,tbl1_setup_add_lag.PEND) then RN else 0 END as gap,
            SUM(gap) OVER (PARTITION BY tbl1_setup_collapse_id ORDER BY tbl1_setup_add_lag.RN) AS gapid

--- group on the gapid to remove duplicate rows and reset start and end dates in a criss-cross(upper left, to bottom right)
--- result above becomes: Jan1,Jan 4 a  


    from tbl1_setup_add_lag )
    --- collapse every row with same gapid into one row 
    , tbl1_setup_collapse as 

        ( 
        select 
        tbl1_setup_collapse_id,min(dbt_valid_from_initial) as dbt_valid_from_initial,max(dbt_valid_to) as dbt_valid_to
        from tbl1_setup_add_gapid
        group by tbl1_setup_collapse_id,gapid
         )
---- group by removes the columns, use split_part to get the values and columns back               
    , tbl1_setup_granularity as (
        select     nullif(split_part(tbl1_setup_collapse_id,'~',1),'THISISANULLVALplaceholdertoberemoved') as UNIQUE_ID_KEY, nullif(split_part(tbl1_setup_collapse_id,'~',2),'THISISANULLVALplaceholdertoberemoved') as STG_TDDICDN_ICD_CODE_SHORT_DESC, nullif(split_part(tbl1_setup_collapse_id,'~',3),'THISISANULLVALplaceholdertoberemoved') as STG_TDDICDN_ICD_CODE_LONG_DESC,  *
        from tbl1_setup_collapse
    )



----------------------filling in any gaps in the dates
--  jan2   jan3      becomes jan2  jan3
--  jan6   jan7              jan3  jan7


,tbl1_add_lag2 as (
SELECT
    *,
    LAG(dbt_valid_to,1) OVER(PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial,dbt_valid_to) AS PREV_dbt_valid_to,
    ROW_NUMBER() OVER(PARTITION BY UNIQUE_ID_KEY ORDER BY dbt_valid_from_initial,dbt_valid_to) AS RN --  for smushing
    FROM
        tbl1_setup_granularity
order by UNIQUE_ID_KEY, dbt_valid_from_initial
)
--- if jan6(next row initial) > jan3(previous_to)
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
,tbl1 as (
--    select  UNIQUE_ID_KEY, dbt_valid_from_2B as  dbt_valid_from, dbt_valid_to from  fill_gap
select *,dbt_valid_from_2B as  dbt_valid_from from  tbl1_fill_gap
)

    
--********************************************************************************
-----------------------------FINAL DATES TABLE union all dates, join to get values
--	unpivot dbt_valid_from,dbt_valid_to into a single column adate, all other columns are gone
, combo_dates as (
    select UNIQUE_ID_KEY, adate from tbl0 unpivot(adate for headers in (dbt_valid_from,dbt_valid_to)) 
		union
	select UNIQUE_ID_KEY, adate from tbl1 unpivot(adate for headers in (dbt_valid_from,dbt_valid_to)) 
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
	tbl0.STG_TDDIDCS_ICD_CODE_STATUS_CODE,tbl0.STG_TDDIDCS_ICD_CODE_STATUS_DESC,tbl0.STG_TDDIDCS_ICD_CODE_STATUS_EFFECTIVE_DATE,tbl0.STG_TDDIDCS_ICD_CODE_STATUS_END_DATE,tbl1.STG_TDDICDN_ICD_CODE_LONG_DESC,tbl1.STG_TDDICDN_ICD_CODE_SHORT_DESC,
	final_dates.dbt_valid_from,final_dates.dbt_valid_to
    from tbl0
        inner join final_dates
        on tbl0.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY and tbl0.dbt_valid_from<final_dates.dbt_valid_to and tbl0.dbt_valid_to>final_dates.dbt_valid_from
            
	left join tbl1 on  tbl1.UNIQUE_ID_KEY = final_dates.UNIQUE_ID_KEY and  tbl1.dbt_valid_from<final_dates.dbt_valid_to and  tbl1.dbt_valid_to>final_dates.dbt_valid_from)
        ------------name back to std names
            , rename as (
                select distinct
                    UNIQUE_ID_KEY, --DEBUG_ID,
                    md5(coalesce(cast(UNIQUE_ID_KEY as varchar ), '') || '|' || coalesce(cast(CURRENT_TIMESTAMP as varchar ), '')) as dbt_scd_id,

                    --data cols
                    
		STG_TDDIDCS_ICD_CODE_STATUS_CODE as ICD_CODE_STATUS_CODE ,
		STG_TDDIDCS_ICD_CODE_STATUS_DESC as ICD_CODE_STATUS_DESC ,
		STG_TDDIDCS_ICD_CODE_STATUS_EFFECTIVE_DATE as ICD_CODE_STATUS_EFFECTIVE_DATE ,
		STG_TDDIDCS_ICD_CODE_STATUS_END_DATE as ICD_CODE_STATUS_END_DATE ,
		STG_TDDICDN_ICD_CODE_LONG_DESC as ICD_CODE_LONG_DESC ,
		STG_TDDICDN_ICD_CODE_SHORT_DESC as ICD_CODE_SHORT_DESC ,

                    to_timestamp_ntz(current_timestamp) as dbt_updated_at,
                    --start end
                    dbt_valid_from,
                    NULLIF(dbt_valid_to,TO_DATE('12/31/2999')) as dbt_valid_to
                    from join_sql
            )
    

--********************************************************************************----------------------------- CREATING COLLAPSE ID OF ALL TRACKED COLUMNS AND MAKING IT UNIQUE TO REMOVE DUPLICATE ROWS
    , RENAME_BUILD_GRANULARITY AS (
        SELECT *,MIN(UNIQUE_ID_KEY|| '~' ||COALESCE(CAST(ICD_CODE_STATUS_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(ICD_CODE_STATUS_DESC AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(ICD_CODE_STATUS_EFFECTIVE_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(ICD_CODE_STATUS_END_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(ICD_CODE_SHORT_DESC AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(ICD_CODE_LONG_DESC AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')) OVER(PARTITION BY UNIQUE_ID_KEY|| '~' ||COALESCE(CAST(ICD_CODE_STATUS_CODE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(ICD_CODE_STATUS_DESC AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(ICD_CODE_STATUS_EFFECTIVE_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(ICD_CODE_STATUS_END_DATE AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(ICD_CODE_SHORT_DESC AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')|| '~' ||COALESCE(CAST(ICD_CODE_LONG_DESC AS VARCHAR ), 'THISISANULLVALPLACEHOLDERTOBEREMOVED')) AS RENAME_COLLAPSE_ID
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
        SELECT     NULLIF(SPLIT_PART(RENAME_COLLAPSE_ID,'~',1),'THISISANULLVALPLACEHOLDERTOBEREMOVED') AS UNIQUE_ID_KEY, NULLIF(SPLIT_PART(RENAME_COLLAPSE_ID,'~',2),'THISISANULLVALPLACEHOLDERTOBEREMOVED') AS ICD_CODE_STATUS_CODE, NULLIF(SPLIT_PART(RENAME_COLLAPSE_ID,'~',3),'THISISANULLVALPLACEHOLDERTOBEREMOVED') AS ICD_CODE_STATUS_DESC, NULLIF(SPLIT_PART(RENAME_COLLAPSE_ID,'~',4),'THISISANULLVALPLACEHOLDERTOBEREMOVED') AS ICD_CODE_STATUS_EFFECTIVE_DATE, NULLIF(SPLIT_PART(RENAME_COLLAPSE_ID,'~',5),'THISISANULLVALPLACEHOLDERTOBEREMOVED') AS ICD_CODE_STATUS_END_DATE, NULLIF(SPLIT_PART(RENAME_COLLAPSE_ID,'~',6),'THISISANULLVALPLACEHOLDERTOBEREMOVED') AS ICD_CODE_SHORT_DESC, NULLIF(SPLIT_PART(RENAME_COLLAPSE_ID,'~',7),'THISISANULLVALPLACEHOLDERTOBEREMOVED') AS ICD_CODE_LONG_DESC,  *
        FROM RENAME_COLLAPSE
    )



, final_sql1 as ( select 
	UNIQUE_ID_KEY,md5(coalesce(cast(UNIQUE_ID_KEY as varchar ), '') || '|' || coalesce(cast(CURRENT_TIMESTAMP as varchar ), '')) as dbt_scd_id
    ,to_timestamp(DBT_VALID_FROM) as DBT_VALID_FROM,to_timestamp(DBT_VALID_TO) as DBT_VALID_TO,to_timestamp(current_timestamp) as DBT_UPDATED_AT
    ,ICD_CODE_STATUS_CODE,ICD_CODE_STATUS_DESC,ICD_CODE_STATUS_EFFECTIVE_DATE
    ,NULLIF(ICD_CODE_STATUS_END_DATE, '9999-12-31'::DATE) as ICD_CODE_STATUS_END_DATE,ICD_CODE_SHORT_DESC,ICD_CODE_LONG_DESC 
from rename_granularity  order by dbt_valid_from,dbt_valid_to )
    

--------PHASE 1: DEBUG
-- set Debug to True to test these

-- select *  from final_sql  -- verify basically works
, ETL_STEP AS (
select 
CASE WHEN UNIQUE_ID_KEY = LEAD(UNIQUE_ID_KEY) OVER (ORDER BY UNIQUE_ID_KEY,DBT_VALID_FROM, DBT_VALID_TO)
    AND ICD_CODE_STATUS_EFFECTIVE_DATE = LEAD (ICD_CODE_STATUS_EFFECTIVE_DATE) OVER (PARTITION BY UNIQUE_ID_KEY ORDER BY DBT_VALID_FROM, DBT_VALID_TO)
    AND DBT_VALID_FROM::DATE +1 NOT BETWEEN ICD_CODE_STATUS_EFFECTIVE_DATE AND NVL(ICD_CODE_STATUS_END_DATE, '12/31/2099') THEN 'Y'
  ELSE 'N' END AS GAP_FLG
, *  from final_sql1 )


, final_sql as
(select 
 UNIQUE_ID_KEY
, DBT_SCD_ID
, case when lag(GAP_FLG)OVER (PARTITION BY UNIQUE_ID_KEY ORDER BY DBT_VALID_FROM, DBT_VALID_TO, ICD_CODE_STATUS_EFFECTIVE_DATE, ICD_CODE_STATUS_END_DATE) = 'Y'
  then DBT_VALID_FROM::DATE -1 else DBT_VALID_FROM end as DBT_VALID_FROM
, case GAP_FLG when 'Y' THEN DBT_VALID_TO::DATE -1 else DBT_VALID_TO end as DBT_VALID_TO
, DBT_UPDATED_AT
, case GAP_FLG when 'Y' THEN NULL ELSE ICD_CODE_STATUS_CODE END AS ICD_CODE_STATUS_CODE
, case GAP_FLG when 'Y' THEN NULL ELSE ICD_CODE_STATUS_DESC END AS ICD_CODE_STATUS_DESC
, case GAP_FLG when 'Y' THEN NULL ELSE ICD_CODE_STATUS_EFFECTIVE_DATE END AS ICD_CODE_STATUS_EFFECTIVE_DATE
, case GAP_FLG when 'Y' THEN NULL ELSE ICD_CODE_STATUS_END_DATE END AS ICD_CODE_STATUS_END_DATE
, ICD_CODE_SHORT_DESC
, ICD_CODE_LONG_DESC
, CONDITIONAL_CHANGE_EVENT(ICD_CODE_STATUS_EFFECTIVE_DATE||ICD_CODE_SHORT_DESC||ICD_CODE_LONG_DESC) OVER (PARTITION BY UNIQUE_ID_KEY ORDER BY DBT_VALID_FROM, DBT_VALID_TO, ICD_CODE_STATUS_EFFECTIVE_DATE, ICD_CODE_STATUS_END_DATE)CC1
from ETL_STEP)


select
UNIQUE_ID_KEY
, DBT_SCD_ID
, MIN(DBT_VALID_FROM) AS DBT_VALID_FROM
, NULLIF(MAX(NVL(DBT_VALID_TO,'9999-12-31'::DATE)), '9999-12-31') AS DBT_VALID_TO
, MAX(DBT_UPDATED_AT) AS DBT_UPDATED_AT
, ICD_CODE_STATUS_CODE
, ICD_CODE_STATUS_DESC
, ICD_CODE_STATUS_EFFECTIVE_DATE
, ICD_CODE_STATUS_END_DATE
, MIN(ICD_CODE_SHORT_DESC) AS ICD_CODE_SHORT_DESC
, MIN(ICD_CODE_LONG_DESC) AS ICD_CODE_LONG_DESC
from final_sql
GROUP BY 1,2,6,7,8,9, CC1
order by 1, 3,4