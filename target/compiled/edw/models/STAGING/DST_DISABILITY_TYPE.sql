---- SRC LAYER ----
WITH
SRC_DIS as ( SELECT *     from     STAGING.STG_CLAIM_DISABILITY_MANAGEMENT ),
//SRC_DIS as ( SELECT *     from     STG_CLAIM_DISABILITY_MANAGEMENT) ,

---- LOGIC LAYER ----

LOGIC_DIS as ( SELECT 
		  TRIM( CLM_DISAB_MANG_DISAB_TYP_CD )                as                        CLM_DISAB_MANG_DISAB_TYP_CD 
		, TRIM( CLM_DISAB_MANG_RSN_TYP_CD )                  as                          CLM_DISAB_MANG_RSN_TYP_CD 
		, TRIM( CLM_DISAB_MANG_MED_STS_TYP_CD )              as                      CLM_DISAB_MANG_MED_STS_TYP_CD 
		, TRIM( CLM_DISAB_MANG_WK_STS_TYP_CD )               as                       CLM_DISAB_MANG_WK_STS_TYP_CD 
		, TRIM( CLM_DISAB_MANG_DISAB_TYP_NM )                as                        CLM_DISAB_MANG_DISAB_TYP_NM 
		, TRIM( CLM_DISAB_MANG_RSN_TYP_NM )                  as                          CLM_DISAB_MANG_RSN_TYP_NM 
		, TRIM( CLM_DISAB_MANG_MED_STS_TYP_NM )              as                      CLM_DISAB_MANG_MED_STS_TYP_NM 
		, TRIM( CLM_DISAB_MANG_WK_STS_TYP_NM )               as                       CLM_DISAB_MANG_WK_STS_TYP_NM 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_DIS
            )

---- RENAME LAYER ----
,

RENAME_DIS as ( SELECT 
		  CLM_DISAB_MANG_DISAB_TYP_CD                        as                        CLM_DISAB_MANG_DISAB_TYP_CD
		, CLM_DISAB_MANG_RSN_TYP_CD                          as                          CLM_DISAB_MANG_RSN_TYP_CD
		, CLM_DISAB_MANG_MED_STS_TYP_CD                      as                      CLM_DISAB_MANG_MED_STS_TYP_CD
		, CLM_DISAB_MANG_WK_STS_TYP_CD                       as                       CLM_DISAB_MANG_WK_STS_TYP_CD
		, CLM_DISAB_MANG_DISAB_TYP_NM                        as                        CLM_DISAB_MANG_DISAB_TYP_NM
		, CLM_DISAB_MANG_RSN_TYP_NM                          as                          CLM_DISAB_MANG_RSN_TYP_NM
		, CLM_DISAB_MANG_MED_STS_TYP_NM                      as                      CLM_DISAB_MANG_MED_STS_TYP_NM
		, CLM_DISAB_MANG_WK_STS_TYP_NM                       as                       CLM_DISAB_MANG_WK_STS_TYP_NM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_DIS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DIS                            as ( SELECT * from    RENAME_DIS 
                                            WHERE VOID_IND = 'N'  ),

---- JOIN LAYER ----

 JOIN_DIS  as  ( SELECT * 
				FROM  FILTER_DIS )
                


,
 
 ETL AS (
 SELECT distinct
CLM_DISAB_MANG_DISAB_TYP_CD
, CLM_DISAB_MANG_RSN_TYP_CD
,CLM_DISAB_MANG_MED_STS_TYP_CD
,CLM_DISAB_MANG_WK_STS_TYP_CD
,CLM_DISAB_MANG_DISAB_TYP_NM
, CLM_DISAB_MANG_RSN_TYP_NM
, CLM_DISAB_MANG_MED_STS_TYP_NM
, CLM_DISAB_MANG_WK_STS_TYP_NM
,VOID_IND 
 
 FROM  JOIN_DIS )

 ,
 
 ETL_UNION AS (
  SELECT 
CLM_DISAB_MANG_DISAB_TYP_CD
, CLM_DISAB_MANG_RSN_TYP_CD
,CLM_DISAB_MANG_MED_STS_TYP_CD
,CLM_DISAB_MANG_WK_STS_TYP_CD
,CLM_DISAB_MANG_DISAB_TYP_NM
, CLM_DISAB_MANG_RSN_TYP_NM
, CLM_DISAB_MANG_MED_STS_TYP_NM
, CLM_DISAB_MANG_WK_STS_TYP_NM
,VOID_IND 
   ,'Y' as CURRENT_DISABILITY_STATUS_IND
from ETL

   union 
   

  SELECT 
CLM_DISAB_MANG_DISAB_TYP_CD
, CLM_DISAB_MANG_RSN_TYP_CD
,CLM_DISAB_MANG_MED_STS_TYP_CD
,CLM_DISAB_MANG_WK_STS_TYP_CD
,CLM_DISAB_MANG_DISAB_TYP_NM
, CLM_DISAB_MANG_RSN_TYP_NM
, CLM_DISAB_MANG_MED_STS_TYP_NM
, CLM_DISAB_MANG_WK_STS_TYP_NM
,VOID_IND 
,'N' as CURRENT_DISABILITY_STATUS_IND
  from ETL
 )

  
  ,
 
 ETL_FINAL AS (
 
 select 
   
 md5(cast(
    
    coalesce(cast(CLM_DISAB_MANG_DISAB_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_DISAB_MANG_RSN_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_DISAB_MANG_MED_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_DISAB_MANG_WK_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CURRENT_DISABILITY_STATUS_IND as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
,CLM_DISAB_MANG_DISAB_TYP_CD
,CLM_DISAB_MANG_RSN_TYP_CD
,CLM_DISAB_MANG_MED_STS_TYP_CD
,CLM_DISAB_MANG_WK_STS_TYP_CD
,CURRENT_DISABILITY_STATUS_IND
,CLM_DISAB_MANG_DISAB_TYP_NM
,CLM_DISAB_MANG_RSN_TYP_NM
,CLM_DISAB_MANG_MED_STS_TYP_NM
,CLM_DISAB_MANG_WK_STS_TYP_NM
,VOID_IND
from ETL_UNION
 )
select * from ETL_FINAL