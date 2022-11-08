---- SRC LAYER ----
WITH
SRC_PCS AS ( SELECT *     from     STAGING.STG_PROVIDER_CERTIFICATION_STATUS ),
SRC_PR AS ( SELECT *     from     STAGING.STG_PROVIDER_RELATION ),
SRC_PR_UN AS ( SELECT *   from     STAGING.STG_PROVIDER_RELATION ),

---- LOGIC LAYER ----

LOGIC_PCS AS ( SELECT 
		  PRVDR_BASE_NMBR || PRVDR_SFX_NMBR                  AS                                 PEACH_NUMBER 
		, TRIM( PRVDR_BASE_NMBR )                            AS                                 PRVDR_BASE_NMBR 
		, TRIM( PRVDR_SFX_NMBR )                             AS                                 PRVDR_SFX_NMBR 
		, TRIM( CRTF_STS_TYPE_CODE )                         AS                                 CRTF_STS_TYPE_CODE 
		, TRIM( CRTF_STS_TYPE_NAME )                         AS                                 CRTF_STS_TYPE_NAME 
		, TRIM( PRVDR_STS_RSN_CODE )                         AS                                 PRVDR_STS_RSN_CODE 
		, TRIM( STS_RSN_TYPE_NAME )                          AS                                 STS_RSN_TYPE_NAME 
		, EFCTV_DATE                                         AS                                 EFCTV_DATE 
		, ENDNG_DATE                                         AS                                 ENDNG_DATE 
		, CSTS_CRT_DTTM                                      AS									CSTS_CRT_DTTM
		, DCTVT_DTTM                                         AS                                 DCTVT_DTTM                                               
		, TRIM( CRT_USER_CODE )                              AS                                 CRT_USER_CODE
		, TRIM( DCTVT_USER_CODE )                            AS                                 DCTVT_USER_CODE
		
				from SRC_PCS
            ),
LOGIC_PR AS ( SELECT 
		  PR_PRVDR_BASE_NMBR || PR_PRVDR_SFX_NMBR            AS                                 PEACH_NUMBER 
		, PR_PRVDR_BASE_NMBR                                 AS                                 PRVDR_BASE_NMBR 
		, PR_PRVDR_SFX_NMBR                                  AS                                 PRVDR_SFX_NMBR 
		, RLTN_CRT_DTTM 				                     AS                                 RLTN_CRT_DTTM 
		, DCTVT_DTTM                          				 AS                                 DCTVT_DTTM 
		, CRT_USER_CODE                                      AS                                 CRT_USER_CODE 
		, DCTVT_USER_CODE                                    AS                                 DCTVT_USER_CODE 
		
		from SRC_PR
            ),
			
LOGIC_PR_UN AS ( SELECT 
           PR_PRVDR_BASE_NMBR || PR_PRVDR_SFX_NMBR           AS                                 PEACH_NUMBER 
		 , PR_PRVDR_BASE_NMBR                                AS                                 PRVDR_BASE_NMBR
         , PR_PRVDR_SFX_NMBR                                 AS                                 PRVDR_SFX_NMBR
         , 'CMBND'                                           AS                                 CRTF_STS_TYPE_CODE
         , 'COMBINED'                                        AS                                 CRTF_STS_TYPE_NAME
         , NULL                                              AS                                 CRTF_STS_RSN_CODE
         , NULL                                              AS                                 CRTF_STS_RSN_NAME
         , CAST( RLTN_CRT_DTTM AS DATE)                      AS                                 STS_EFCTV_DATE
         , CAST(DCTVT_DTTM AS DATE)                          AS                                 STS_ENDNG_DATE
         , CAST(RLTN_CRT_DTTM AS DATE)                       AS                                 DRVD_EFCTV_DATE
         , CASE 
when DCTVT_DTTM > current_date() then null else CAST(DCTVT_DTTM AS DATE) end AS                 DCTVT_DTTM
         , CRT_USER_CODE                                     AS                                 CRT_USER_CODE
         , CASE 
when DCTVT_DTTM > current_date() then null else DCTVT_USER_CODE end AS                          DCTVT_USER_CODE
           from SRC_PR_UN
            ),
---- RENAME LAYER ----


RENAME_PCS AS ( SELECT 
		  PEACH_NUMBER                                       AS                                 PEACH_NUMBER
		, PRVDR_BASE_NMBR                                    AS                                 PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     AS                                 PRVDR_SFX_NMBR
		, CRTF_STS_TYPE_CODE                                 AS                                 CRTF_STS_TYPE_CODE
		, CRTF_STS_TYPE_NAME                                 AS                                 CRTF_STS_TYPE_NAME
		, PRVDR_STS_RSN_CODE                                 AS                                 CRTF_STS_RSN_CODE
		, STS_RSN_TYPE_NAME                                  AS                                 CRTF_STS_RSN_NAME
		, EFCTV_DATE                                         AS                                 STS_EFCTV_DATE
		, ENDNG_DATE                                         AS                                 STS_ENDNG_DATE 
		, CSTS_CRT_DTTM                                      AS                                 CSTS_CRT_DTTM
		, DCTVT_DTTM                                         AS                                 PCS_DCTVT_DTTM
		, CRT_USER_CODE                                      AS                                 CRT_USER_CODE
		, DCTVT_USER_CODE                                    AS                                 DCTVT_USER_CODE		
				
					FROM     LOGIC_PCS   ), 

RENAME_PR AS ( SELECT 
		  PEACH_NUMBER                                       AS                                 PR_PEACH_NUMBER
		, PRVDR_BASE_NMBR                                    AS                                 PR_PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     AS                                 PR_PRVDR_SFX_NMBR
		, RLTN_CRT_DTTM                                      AS                                 PR_STS_EFCTV_DATE
		, DCTVT_DTTM                                         AS                                 PR_STS_ENDNG_DATE
		, RLTN_CRT_DTTM                                      AS                                 DRVD_EFCTV_DATE
		, DCTVT_DTTM                                         AS                                 DRVD_ENDNG_DATE
		, CRT_USER_CODE                                      AS                                 DRVD_EFCTV_USER_CODE
		, DCTVT_USER_CODE                                    AS                                 DRVD_ENDNG_USER_CODE 
				
				FROM     LOGIC_PR   ),
RENAME_PR_UN as ( SELECT 
		  PEACH_NUMBER                                       AS                                 PEACH_NUMBER
		, PRVDR_BASE_NMBR                                    AS                                 PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     AS                                 PRVDR_SFX_NMBR
		, CRTF_STS_TYPE_CODE                                 AS                                 CRTF_STS_TYPE_CODE
		, CRTF_STS_TYPE_NAME                                 AS                                 CRTF_STS_TYPE_NAME
		, CRTF_STS_RSN_CODE                                  AS                                 CRTF_STS_RSN_CODE
		, CRTF_STS_RSN_NAME                                  AS                                 CRTF_STS_RSN_NAME
		, STS_EFCTV_DATE                                     AS                                 STS_EFCTV_DATE                                                                            
		, STS_ENDNG_DATE                                     AS                                 STS_ENDNG_DATE                                                                          
		, DRVD_EFCTV_DATE                                    AS                                 DRVD_EFCTV_DATE                                                                      
		, DCTVT_DTTM                                         AS                                 DRVD_ENDNG_DATE                                                                     
		, CRT_USER_CODE                                      AS                                 DRVD_EFCTV_USER_CODE                                                           
		, DCTVT_USER_CODE                                    AS                                 DRVD_ENDNG_USER_CODE 

        FROM     LOGIC_PR_UN   )				

---- FILTER LAYER (uses aliases) ----
,
		  FILTER_PCS                                         AS 				( SELECT * from    RENAME_PCS   ),
		  FILTER_PR                             			 AS 				( SELECT * from    RENAME_PR   ),
		  FILTER_PR_UN                             			 AS 				( SELECT * from    RENAME_PR_UN   ),


--LOGIC FOR DERIVED EFFECTIVE/END DATES:

--1. Exclude records where the End Date is before the Effective Date
--2. Exclude records that were created and deactivated within the same day.
--3. Exclude IN PROCESS records that were effective for less than one day.
--4. Find row pairs (Replaced vs. Replacement) by using: Partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR,Order by CSTS_CRT_DTTM, EFCTV_DATE, ENDNG_DATE
--			a. Replaced rows will be Deactivated (DCTVT_DTTM < current_date)
--			b. Replacement rows will be Active (DCTVT_DTTM > current_date)
--			c. Both rows will share the same EFCTV_DATE, CRTF_STS_TYPE_CODE, PRVDR_RSN_STS_CODE
--			d. We need to retrive the CSTS_CRT_DTTM, EFCTV_DATE, and CRT_USER_CODE from the Replaced row.
--5. Filter out all Replaced rows.
--6. Remove all rows where Derived End Date is before Derived Effective Date.
--7. Add rows for providers that were combined.




---- JOIN LAYER ----

PCS AS ( 

SELECT 
PRVDR_BASE_NMBR||PRVDR_SFX_NMBR 		AS PEACH_NUMBER,
PRVDR_BASE_NMBR, 
PRVDR_SFX_NMBR, 
FILTER_PCS.CSTS_CRT_DTTM 				AS CREATE_DTTM,
date(FILTER_PCS.PCS_DCTVT_DTTM) 		AS DCTVT_DATE, 
date(FILTER_PR.DRVD_EFCTV_DATE) 		AS RLTN_CRT_DATE, 
FILTER_PR.DRVD_EFCTV_USER_CODE 			AS RLTN_CRT_USER_CODE,
FILTER_PCS.CRT_USER_CODE,
FILTER_PCS.DCTVT_USER_CODE,
FILTER_PCS.CRTF_STS_TYPE_CODE ,
FILTER_PCS.CRTF_STS_TYPE_NAME ,
FILTER_PCS.CRTF_STS_RSN_CODE ,
FILTER_PCS.CRTF_STS_RSN_NAME ,
FILTER_PCS.STS_EFCTV_DATE ,  
FILTER_PCS.STS_ENDNG_DATE,
date(FILTER_PCS.CSTS_CRT_DTTM) 			AS CREATE_DATE,

case when CRTF_STS_TYPE_NAME = lag(CRTF_STS_TYPE_NAME) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
		order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) and STS_EFCTV_DATE = lag(STS_EFCTV_DATE) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
		order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) and lag(FILTER_PCS.PCS_DCTVT_DTTM) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
		order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) <= current_date 
		then lag(date(FILTER_PCS.CSTS_CRT_DTTM)) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
		order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) else null 
		end as PREV_CREATE_DATE,

case when CRTF_STS_TYPE_NAME = lag(CRTF_STS_TYPE_NAME) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
		order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) and STS_EFCTV_DATE = lag(STS_EFCTV_DATE) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
	    order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) and lag(FILTER_PCS.PCS_DCTVT_DTTM) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
	    order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) <= current_date 
		then lag(STS_EFCTV_DATE) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
		order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) else null 
		end as PREV_EFCTV_DATE,

case when CRTF_STS_TYPE_NAME = lag(CRTF_STS_TYPE_NAME) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
		order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) and STS_EFCTV_DATE = lag(STS_EFCTV_DATE) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
		order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) and lag(FILTER_PCS.PCS_DCTVT_DTTM) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
		order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) <= current_date 
		then lag(FILTER_PCS.CRT_USER_CODE) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
		order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) else null 
		end as PREV_CRT_USER_CODE,

case when CRTF_STS_TYPE_NAME = lead(CRTF_STS_TYPE_NAME) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
		order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) 
		and STS_EFCTV_DATE = lead(STS_EFCTV_DATE) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
	    order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) 
		and FILTER_PCS.PCS_DCTVT_DTTM <= current_date 
		then 'Y' 
		else 'N' 
		end as REPLACED, --flag deactivated rows that were replaced by an active row

case when CRTF_STS_TYPE_CODE = 'INPRO' 
		and STS_EFCTV_DATE = STS_ENDNG_DATE 
		then 'Y' 
		else 'N' 
		end as INPRO_1DAY,

case when CRTF_STS_TYPE_CODE = 'INPRO' 
		and STS_ENDNG_DATE > current_date 
		and FILTER_PR.DRVD_EFCTV_DATE is not null 
		then 'Y' 
		when CRTF_STS_TYPE_CODE = 'CRTF' 
		and  CRTF_STS_RSN_CODE = 'SCERR' 
		and STS_ENDNG_DATE > current_date 
		and FILTER_PR.DRVD_EFCTV_DATE is not null 
		then 'Y' 
		else 'N' 
		end as CMBND_REMOVE,

case when STS_ENDNG_DATE > current_date 
		and FILTER_PCS.PCS_DCTVT_DTTM > current_date 
		then 'Y' 
		else 'N' 
		end as CRNT,

case when STS_ENDNG_DATE > current_date 
		and FILTER_PCS.CSTS_CRT_DTTM = max(FILTER_PCS.CSTS_CRT_DTTM) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR) 
		then 'Y' 
		else 'N' 
		end as LST

FROM  FILTER_PCS FILTER_PCS

left join FILTER_PR FILTER_PR on FILTER_PCS.PRVDR_BASE_NMBR = FILTER_PR.PR_PRVDR_BASE_NMBR 
  and FILTER_PCS.PRVDR_SFX_NMBR = FILTER_PR.PR_PRVDR_SFX_NMBR 
  and FILTER_PR.DRVD_ENDNG_DATE > current_date
  where STS_EFCTV_DATE <= STS_ENDNG_DATE
  order by PEACH_NUMBER, CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE,CRTF_STS_TYPE_CODE, CRTF_STS_TYPE_NAME, CRTF_STS_RSN_CODE,
  CRTF_STS_RSN_NAME,STS_EFCTV_DATE, STS_ENDNG_DATE,date(CSTS_CRT_DTTM)
   ),
   
PCS2 AS (
select 
PEACH_NUMBER, 
PRVDR_BASE_NMBR, 
PRVDR_SFX_NMBR,   
CRTF_STS_TYPE_CODE, 
CRTF_STS_TYPE_NAME, 
CRTF_STS_RSN_CODE, 
CRTF_STS_RSN_NAME, 
STS_EFCTV_DATE, 
STS_ENDNG_DATE,

case when PREV_CREATE_DATE is null 
	  then CREATE_DATE
      when CREATE_DTTM = min(CREATE_DTTM) over (partition by PEACH_NUMBER) 
	  and PREV_EFCTV_DATE < PREV_CREATE_DATE 
	  then PREV_EFCTV_DATE 
	  else PREV_CREATE_DATE
      end as HIST_EFCTV_DATE,

case when STS_ENDNG_DATE > current_date and RLTN_CRT_DATE is not null 
	 and LST = 'Y' 
	 then RLTN_CRT_DATE - 1
     when STS_ENDNG_DATE > current_date and DCTVT_DATE > current_date then null 
     when CREATE_DATE = lead(CREATE_DATE) over (partition by PEACH_NUMBER 
     order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) 
	 then CREATE_DATE - 1
     when CREATE_DATE = lead(PREV_CREATE_DATE) over (partition by PEACH_NUMBER 
     order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) 
	 then CREATE_DATE - 1
     else lead(coalesce(PREV_CREATE_DATE,CREATE_DATE)) over (partition by PEACH_NUMBER 
     order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) - 1 
	 end as HIST_ENDNG_DATE,
                              
coalesce(PREV_CRT_USER_CODE, CRT_USER_CODE) as HIST_EFCTV_USER_CODE, 

case when STS_ENDNG_DATE > current_date and RLTN_CRT_DATE is not null 
     then RLTN_CRT_USER_CODE
     when STS_ENDNG_DATE > current_date then null 
     else CRT_USER_CODE 
	 end as HIST_ENDNG_USER_CODE

from PCS
where CRNT = 'Y' or (REPLACED = 'N' and INPRO_1DAY = 'N' and CMBND_REMOVE = 'N')),

PCS_UNION AS (

select 
PEACH_NUMBER, 
PRVDR_BASE_NMBR, 
PRVDR_SFX_NMBR, 
CRTF_STS_TYPE_CODE, 
CRTF_STS_TYPE_NAME, 
CRTF_STS_RSN_CODE, 
CRTF_STS_RSN_NAME, 
STS_EFCTV_DATE, 
STS_ENDNG_DATE,
HIST_EFCTV_DATE,
HIST_ENDNG_DATE,
HIST_EFCTV_USER_CODE, 
HIST_ENDNG_USER_CODE
from PCS2
  
  UNION 
  
select
PEACH_NUMBER,
PRVDR_BASE_NMBR, 
PRVDR_SFX_NMBR, 
'CMBND' as CRTF_STS_TYPE_CODE, 
'COMBINED' as CRTF_STS_TYPE_NAME, 
NULL as CRTF_STS_RSN_CODE, 
NULL as CRTF_STS_RSN_NAME,
STS_EFCTV_DATE, 
STS_ENDNG_DATE,
STS_EFCTV_DATE AS HIST_EFCTV_DATE, 
case when STS_ENDNG_DATE > current_date then null else date(STS_ENDNG_DATE) end as HIST_ENDNG_DATE,
DRVD_EFCTV_USER_CODE as HIST_EFCTV_USER_CODE,
case when STS_ENDNG_DATE > current_date then null else DRVD_ENDNG_USER_CODE end as HIST_ENDNG_USER_CODE
from FILTER_PR_UN
),

APPLY_FILTER AS ( SELECT * FROM PCS_UNION
               WHERE HIST_EFCTV_DATE <= HIST_ENDNG_DATE or HIST_ENDNG_DATE is null ),

--RENAME COL

RENAME_COL AS (
SELECT 
PEACH_NUMBER
,PRVDR_BASE_NMBR
,PRVDR_SFX_NMBR
,CRTF_STS_TYPE_CODE
,CRTF_STS_TYPE_NAME
,CRTF_STS_RSN_CODE
,CRTF_STS_RSN_NAME
,STS_EFCTV_DATE
,STS_ENDNG_DATE
,HIST_EFCTV_DATE AS DRVD_EFCTV_DATE
,HIST_ENDNG_DATE AS DRVD_ENDNG_DATE
,HIST_EFCTV_USER_CODE AS DRVD_EFCTV_USER_CODE
,HIST_ENDNG_USER_CODE AS DRVD_ENDNG_USER_CODE

from APPLY_FILTER
),

--UNIQUE_ID KEY
FINAL_SQL AS ( SELECT 
md5(cast(
    
    coalesce(cast(PEACH_NUMBER as 
    varchar
), '') || '-' || coalesce(cast(DRVD_EFCTV_DATE as 
    varchar
), '')

 as 
    varchar
))   as  UNIQUE_ID_KEY
,PEACH_NUMBER
,PRVDR_BASE_NMBR
,PRVDR_SFX_NMBR
,CRTF_STS_TYPE_CODE
,CRTF_STS_TYPE_NAME
,CRTF_STS_RSN_CODE
,CRTF_STS_RSN_NAME
,STS_EFCTV_DATE
,STS_ENDNG_DATE
,DRVD_EFCTV_DATE
,DRVD_ENDNG_DATE
,DRVD_EFCTV_USER_CODE
,DRVD_ENDNG_USER_CODE
FROM RENAME_COL
)

SELECT * FROM FINAL_SQL