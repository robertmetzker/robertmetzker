

      create or replace  table DEV_EDW.STAGING.DST_PROVIDER_ENROLLMENT_STATUS_LOG  as
      (WITH
SRC_PES as ( SELECT *     from     STAGING.STG_PROVIDER_ENROLLMENT_STATUS ),
SRC_PR as ( SELECT *      from     STAGING.STG_PROVIDER_RELATION ),
SRC_PR_UN as ( SELECT *   from     STAGING.STG_PROVIDER_RELATION ),
//SRC_PES as ( SELECT *   from     STG_PROVIDER_ENROLLMENT_STATUS) ,
//SRC_PR as ( SELECT *    from     STG_PROVIDER_RELATION) ,
//SRC_PR_UN as ( SELECT * from     STG_PROVIDER_RELATION) ,

---- LOGIC LAYER ----

LOGIC_PES as ( SELECT 
  PRVDR_BASE_NMBR || PRVDR_SFX_NMBR                 AS                                        PEACH_NUMBER 
, TRIM( PRVDR_BASE_NMBR )                            AS                                        PRVDR_BASE_NMBR 
, TRIM( PRVDR_SFX_NMBR )                             AS                                        PRVDR_SFX_NMBR 
, TRIM( ENRL_STS_TYPE_CODE )                         AS                                        ENRL_STS_TYPE_CODE 
, TRIM( ENRL_STS_NAME )                              AS                                        ENRL_STS_NAME 
, TRIM( PRVDR_STS_RSN_CODE )                         AS                                        PRVDR_STS_RSN_CODE 
, TRIM( STS_RSN_TYPE_NAME )                          AS                                        STS_RSN_TYPE_NAME 
, EFCTV_DATE                                         AS                                        EFCTV_DATE 
, ENDNG_DATE                                         AS                                        ENDNG_DATE 
, ESTS_CRT_DTTM                                      AS                                        ESTS_CRT_DTTM
, DCTVT_DTTM                                         AS                                        DCTVT_DTTM                                               
, CRT_USER_CODE                                      AS                                        CRT_USER_CODE
, DCTVT_USER_CODE                                    AS                                        DCTVT_USER_CODE
              
              from SRC_PES
            ),
LOGIC_PR as ( SELECT 
  PR_PRVDR_BASE_NMBR || PR_PRVDR_SFX_NMBR          AS                                       PEACH_NUMBER 
, PR_PRVDR_BASE_NMBR                                AS                                       PRVDR_BASE_NMBR
, PR_PRVDR_SFX_NMBR                                 AS                                       PRVDR_SFX_NMBR
, RLTN_CRT_DTTM                                     AS                                       RLTN_CRT_DTTM
, DCTVT_DTTM                                        AS                                       DCTVT_DTTM 
, CRT_USER_CODE                                     AS                                       CRT_USER_CODE
, DCTVT_USER_CODE                                   AS                                       DCTVT_USER_CODE


        from SRC_PR
            ),
                                                                                          
LOGIC_PR_UN as ( SELECT 
 PR_PRVDR_BASE_NMBR || PR_PRVDR_SFX_NMBR           AS                                       PEACH_NUMBER 
, PR_PRVDR_BASE_NMBR                                AS                                       PRVDR_BASE_NMBR
, PR_PRVDR_SFX_NMBR                                 AS                                       PRVDR_SFX_NMBR
, 'CMBND'                                           AS                                       ENRL_STS_TYPE_CODE
, 'COMBINED'                                        AS                                       ENRL_STS_TYPE_NAME
, NULL                                              AS                                       ENRL_STS_RSN_CODE
, NULL                                              AS                                       ENRL_STS_RSN_NAME
, CAST( RLTN_CRT_DTTM AS DATE)                      AS                                       STS_EFCTV_DATE
, CAST(DCTVT_DTTM AS DATE)                          AS                                       STS_ENDNG_DATE
, CAST(RLTN_CRT_DTTM AS DATE)                       AS                                       DRVD_EFCTV_DATE
, CASE 
  when DCTVT_DTTM > current_date() then null else CAST(DCTVT_DTTM AS DATE) end as            DCTVT_DTTM
, CRT_USER_CODE                                     AS                                       CRT_USER_CODE
, CASE 
  when DCTVT_DTTM > current_date() then null else DCTVT_USER_CODE end as                     DCTVT_USER_CODE
           from SRC_PR_UN
            ),
---- RENAME LAYER ----


RENAME_PES as ( SELECT 
 PEACH_NUMBER                                       AS                                       PEACH_NUMBER
, PRVDR_BASE_NMBR                                    AS                                       PRVDR_BASE_NMBR
, PRVDR_SFX_NMBR                                     AS                                       PRVDR_SFX_NMBR
, ENRL_STS_TYPE_CODE                                 AS                                       ENRL_STS_TYPE_CODE
, ENRL_STS_NAME                                      AS                                       ENRL_STS_TYPE_NAME
, PRVDR_STS_RSN_CODE                                 AS                                       ENRL_STS_RSN_CODE
, STS_RSN_TYPE_NAME                                  AS                                       ENRL_STS_RSN_NAME
, EFCTV_DATE                                         AS                                       STS_EFCTV_DATE
, ENDNG_DATE                                         AS                                       STS_ENDNG_DATE 
, ESTS_CRT_DTTM                                      AS                                       ESTS_CRT_DTTM
, DCTVT_DTTM                                         AS                                       PES_DCTVT_DTTM
, CRT_USER_CODE                                      AS                                       CRT_USER_CODE
, DCTVT_USER_CODE                                    AS                                       DCTVT_USER_CODE
               
               FROM     LOGIC_PES   ), 
                                                            



RENAME_PR as ( SELECT 
 PEACH_NUMBER                                          AS                                    PR_PEACH_NUMBER
, PRVDR_BASE_NMBR                                       AS                                    PR_PRVDR_BASE_NMBR
, PRVDR_SFX_NMBR                                        AS                                    PR_PRVDR_SFX_NMBR
, RLTN_CRT_DTTM                                         AS                                    PR_STS_EFCTV_DATE                                                                            
, DCTVT_DTTM                                            AS                                    PR_STS_ENDNG_DATE                                                                          
, RLTN_CRT_DTTM                                         AS                                    DRVD_EFCTV_DATE                                                                      
, DCTVT_DTTM                                            AS                                    DRVD_ENDNG_DATE                                                                     
, CRT_USER_CODE                                         AS                                    DRVD_EFCTV_USER_CODE                                                           
, DCTVT_USER_CODE                                       AS                                    DRVD_ENDNG_USER_CODE 

         FROM     LOGIC_PR   ),
RENAME_PR_UN as ( SELECT 
 PEACH_NUMBER                                     AS                                    PEACH_NUMBER
, PRVDR_BASE_NMBR                                  AS                                    PRVDR_BASE_NMBR
, PRVDR_SFX_NMBR                                   AS                                    PRVDR_SFX_NMBR
, ENRL_STS_TYPE_CODE                               AS                                    ENRL_STS_TYPE_CODE
, ENRL_STS_TYPE_NAME                               AS                                    ENRL_STS_TYPE_NAME
, ENRL_STS_RSN_CODE                                AS                                    ENRL_STS_RSN_CODE
, ENRL_STS_RSN_NAME                                AS                                    ENRL_STS_RSN_NAME
, STS_EFCTV_DATE                                   AS                                    STS_EFCTV_DATE                                                                            
, STS_ENDNG_DATE                                   AS                                    STS_ENDNG_DATE                                                                          
, DRVD_EFCTV_DATE                                  AS                                    DRVD_EFCTV_DATE                                                                      
, DCTVT_DTTM                                       AS                                    DRVD_ENDNG_DATE                                                                     
, CRT_USER_CODE                                    AS                                    DRVD_EFCTV_USER_CODE                                                           
, DCTVT_USER_CODE                                  AS                                    DRVD_ENDNG_USER_CODE 

        FROM     LOGIC_PR_UN   )
                                                            
---- FILTER LAYER (uses aliases) ----
,
FILTER_PES                            as ( SELECT * from    RENAME_PES   ),
FILTER_PR                             as ( SELECT * from    RENAME_PR   ),
FILTER_PR_UN                             as ( SELECT * from    RENAME_PR_UN   ),


--LOGIC FOR DERIVED EFFECTIVE/END DATES:

--1. Exclude records where the End Date is before the Effective Date
--2. Exclude records that were created and deactivated within the same day.
--3. Exclude IN PROCESS records that were effective for less than one day.
--4. Find row pairs (Replaced vs. Replacement) by using: Partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR ,Order by CESTS_CRT_DTTM, EFCTV_DATE, ENDNG_DATE
--       a.Replaced rows will be Deactivated (DCTVT_DTTM < current_date)
--       b.Replacement rows will be Active (DCTVT_DTTM > current_date)
--       c.Both rows will share the same EFCTV_DATE, ENRL_STS_TYPE_CODE, PRVDR_RSN_STS_CODE
--       d.We need to retrive the ESTS_CRT_DTTM, EFCTV_DATE, and CRT_USER_CODE from the Replaced row.
--5. Filter out all Replaced rows.
--6. Remove all rows where Derived End Date is before Derived Effective Date.
--7. Filter out these:
--        a. ENRL_STS_TYPE_CODE = 'DACTV' (Deactivated) and PRVDR_STS_RSN_CODE = 'ADMDC' (Administrative Termination) and DCTVT_DTTM < current date (voided) and RLTN_CRT_DTTM is null (not combined) and ESTS_CRT_DTTM > min(ESTS_CRT_DTTM) (not the first row for the provider)
--        b. DCTVT_DTTM < current date (voided) and DCTVT_DTTM  < max(DCTVT_DTTM) (not last row for the provider) and ESTS_CRT_DTTM < min(ESTS_CRT_DTTM) (not first row for the provider)
--8. Add rows for providers that were combined.

---- JOIN LAYER ----

PES as ( 
  
  select 

PEACH_NUMBER,
PRVDR_BASE_NMBR, 
PRVDR_SFX_NMBR, 
FILTER_PES.ESTS_CRT_DTTM as CREATE_DTTM,
date(FILTER_PES.PES_DCTVT_DTTM) as DCTVT_DATE, 
date(FILTER_PR.DRVD_EFCTV_DATE) as RLTN_CRT_DATE, 
FILTER_PR.DRVD_EFCTV_USER_CODE as RLTN_CRT_USER_CODE,
FILTER_PES.CRT_USER_CODE,
FILTER_PES.DCTVT_USER_CODE,
FILTER_PES.ENRL_STS_TYPE_CODE ,
FILTER_PES.ENRL_STS_TYPE_NAME ,
FILTER_PES.ENRL_STS_RSN_CODE ,
FILTER_PES.ENRL_STS_RSN_NAME ,
FILTER_PES.STS_EFCTV_DATE ,  
FILTER_PES.STS_ENDNG_DATE,
date(FILTER_PES.ESTS_CRT_DTTM) as CREATE_DATE,
  
case when ENRL_STS_TYPE_NAME = lag(ENRL_STS_TYPE_NAME) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
          order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE)
     and STS_EFCTV_DATE = lag(STS_EFCTV_DATE) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
                     order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE)
     and lag(FILTER_PES.PES_DCTVT_DTTM) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
                     order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) <= current_date
    then lag(date(FILTER_PES.ESTS_CRT_DTTM)) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
                     order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) else null 
  end as PREV_CREATE_DATE,
                                
case when ENRL_STS_TYPE_NAME = lag(ENRL_STS_TYPE_NAME) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
         order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE)
    and STS_EFCTV_DATE = lag(STS_EFCTV_DATE) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
                    order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE)
    and lag(FILTER_PES.PES_DCTVT_DTTM) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
                    order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) <= current_date
    then lag(STS_EFCTV_DATE) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
                    order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) else null 
  end as PREV_EFCTV_DATE,
                              
case when ENRL_STS_TYPE_NAME = lag(ENRL_STS_TYPE_NAME) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
         order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE)
    and STS_EFCTV_DATE = lag(STS_EFCTV_DATE) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
                    order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE)
    and lag(FILTER_PES.PES_DCTVT_DTTM) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
                    order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) <= current_date
    then lag(FILTER_PES.CRT_USER_CODE) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
                    order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) else null 
  end as PREV_CRT_USER_CODE,
                              
case when ENRL_STS_TYPE_NAME = lead(ENRL_STS_TYPE_NAME) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
                    order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE)
    and STS_EFCTV_DATE = lead(STS_EFCTV_DATE) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR 
                    order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE)
    and FILTER_PES.PES_DCTVT_DTTM <= current_date
    then 'Y' else 'N' end as REPLACED, --flag deactivated rows that were replaced by an active row
               
case when ENRL_STS_TYPE_CODE = 'INPRO' and STS_EFCTV_DATE = STS_ENDNG_DATE 
                then 'Y' else 'N' end as INPRO_1DAY,

case when ENRL_STS_TYPE_CODE = 'INPRO' 
     and STS_ENDNG_DATE > current_date  and FILTER_PR.DRVD_EFCTV_DATE is not null then 'Y' 
     when ENRL_STS_TYPE_CODE = 'DACTV' and ENRL_STS_RSN_CODE = 'ADMDC' and DCTVT_DATE < current_date and FILTER_PR.DRVD_EFCTV_DATE is null and CREATE_DTTM > min(CREATE_DTTM) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR) 
                then 'Y' 
  when DCTVT_DATE < current_date and pes_DCTVT_DTTM < max(date(pes_DCTVT_DTTM)) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR) and CREATE_DTTM > min(CREATE_DTTM) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR) then 'Y'
  
     else 'N' end as CMBND_REMOVE,
               
case when STS_ENDNG_DATE > current_date 
     and FILTER_PES.PES_DCTVT_DTTM > current_date 
                then 'Y' 
                else 'N' 
                end as CRNT,
               
case when STS_ENDNG_DATE > current_date 
     and FILTER_PES.ESTS_CRT_DTTM = max(FILTER_PES.ESTS_CRT_DTTM) over (partition by PRVDR_BASE_NMBR, PRVDR_SFX_NMBR) 
                then 'Y' else 'N' end as LST
               
from FILTER_PES FILTER_PES
left join FILTER_PR FILTER_PR on FILTER_PES.PRVDR_BASE_NMBR = FILTER_PR.PR_PRVDR_BASE_NMBR 
  and FILTER_PES.PRVDR_SFX_NMBR = FILTER_PR.PR_PRVDR_SFX_NMBR and FILTER_PR.DRVD_ENDNG_DATE > current_date
where STS_EFCTV_DATE <= STS_ENDNG_DATE
order by PEACH_NUMBER, CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE,ENRL_STS_TYPE_CODE, ENRL_STS_TYPE_NAME, ENRL_STS_RSN_CODE,
  ENRL_STS_RSN_NAME,STS_EFCTV_DATE, STS_ENDNG_DATE,date(ESTS_CRT_DTTM)

  
  ),
  
PES2 AS (
select 

PEACH_NUMBER, 
PRVDR_BASE_NMBR, 
PRVDR_SFX_NMBR,   
ENRL_STS_TYPE_CODE, 
ENRL_STS_TYPE_NAME, 
ENRL_STS_RSN_CODE, 
ENRL_STS_RSN_NAME, 
STS_EFCTV_DATE, 
STS_ENDNG_DATE,
case when PREV_CREATE_DATE is null then CREATE_DATE
    when CREATE_DTTM = min(CREATE_DTTM) over (partition by PEACH_NUMBER) and PREV_EFCTV_DATE < PREV_CREATE_DATE then PREV_EFCTV_DATE else PREV_CREATE_DATE
               end as HIST_EFCTV_DATE,
case when STS_ENDNG_DATE > current_date and RLTN_CRT_DATE is not null and LST = 'Y' then RLTN_CRT_DATE - 1
    when STS_ENDNG_DATE > current_date and DCTVT_DATE > current_date then null 
    //when PREV_CREATE_DATE is null and CRTF_STS_TYPE_CODE = 'CRTF' then CREATE_DATE
    when CREATE_DATE = lead(CREATE_DATE) over (partition by PEACH_NUMBER 
                    order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) then CREATE_DATE - 1
    when CREATE_DATE = lead(PREV_CREATE_DATE) over (partition by PEACH_NUMBER 
                    order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) then CREATE_DATE - 1
    else lead(coalesce(PREV_CREATE_DATE,CREATE_DATE)) over (partition by PEACH_NUMBER 
                    order by CREATE_DTTM, STS_EFCTV_DATE, STS_ENDNG_DATE) - 1 end as HIST_ENDNG_DATE,
                              
coalesce(PREV_CRT_USER_CODE, CRT_USER_CODE) as HIST_EFCTV_USER_CODE, 

case when STS_ENDNG_DATE > current_date and RLTN_CRT_DATE is not null then RLTN_CRT_USER_CODE
     when STS_ENDNG_DATE > current_date then null 
     else CRT_USER_CODE end as HIST_ENDNG_USER_CODE
from PES
where CRNT = 'Y' or (REPLACED = 'N' and INPRO_1DAY = 'N' and CMBND_REMOVE = 'N')
  
)  ,

PES_UNION AS (

select 

PEACH_NUMBER, 
PRVDR_BASE_NMBR, 
PRVDR_SFX_NMBR, 
ENRL_STS_TYPE_CODE, 
ENRL_STS_TYPE_NAME, 
ENRL_STS_RSN_CODE, 
ENRL_STS_RSN_NAME, 
STS_EFCTV_DATE, 
STS_ENDNG_DATE,
HIST_EFCTV_DATE,
HIST_ENDNG_DATE,
HIST_EFCTV_USER_CODE, 
HIST_ENDNG_USER_CODE
from PES2
  
UNION 
  
select  

PEACH_NUMBER,
PRVDR_BASE_NMBR, 
PRVDR_SFX_NMBR, 
'CMBND' as ENRL_STS_TYPE_CODE, 
'COMBINED' as ENRL_STS_TYPE_NAME, 
 NULL as ENRL_STS_RSN_CODE, 
 NULL as ENRL_STS_RSN_NAME,
STS_EFCTV_DATE, 
 STS_ENDNG_DATE,
STS_EFCTV_DATE AS HIST_EFCTV_DATE, 
case when STS_ENDNG_DATE > current_date then null else date(STS_ENDNG_DATE) end as HIST_ENDNG_DATE,
DRVD_EFCTV_USER_CODE as HIST_EFCTV_USER_CODE,
case when STS_ENDNG_DATE > current_date then null else DRVD_ENDNG_USER_CODE end as HIST_ENDNG_USER_CODE
from FILTER_PR_UN

),

APPLY_FILTER AS ( SELECT * FROM PES_UNION
               WHERE HIST_EFCTV_DATE <= HIST_ENDNG_DATE or HIST_ENDNG_DATE is null ),


-- Adding a unique id key column
FINAL_SQL AS (
select 
md5(cast(
    
    coalesce(cast(PEACH_NUMBER as 
    varchar
), '') || '-' || coalesce(cast(HIST_EFCTV_DATE as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY
,PEACH_NUMBER
,PRVDR_BASE_NMBR
,PRVDR_SFX_NMBR
,ENRL_STS_TYPE_CODE
,ENRL_STS_TYPE_NAME
,ENRL_STS_RSN_CODE
,ENRL_STS_RSN_NAME
,STS_EFCTV_DATE
,STS_ENDNG_DATE
,HIST_EFCTV_DATE AS DRVD_EFCTV_DATE
,HIST_ENDNG_DATE AS DRVD_ENDNG_DATE
,HIST_EFCTV_USER_CODE AS DRVD_EFCTV_USER_CODE
,HIST_ENDNG_USER_CODE AS DRVD_ENDNG_USER_CODE
from APPLY_FILTER
)
select * from FINAL_SQL
      );
    