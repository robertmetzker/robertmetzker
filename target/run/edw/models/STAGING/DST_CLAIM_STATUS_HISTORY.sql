

      create or replace  table DEV_EDW.STAGING.DST_CLAIM_STATUS_HISTORY  as
      (---- SRC LAYER ----
WITH
SRC_CSH as ( SELECT *     from     STAGING.STG_CLAIM_STATUS_HISTORY ),
//SRC_CSH as ( SELECT *     from     STG_CLAIM_STATUS_HISTORY) ,

---- LOGIC LAYER ----

LOGIC_CSH as ( SELECT 
		  HIST_ID                                            AS                                            HIST_ID 
		, CLM_CLM_STS_ID                                     AS                                     CLM_CLM_STS_ID 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, CLM_CLM_STS_STT_DT                                 AS                                 CLM_CLM_STS_STT_DT 
		, CLM_CLM_STS_EFF_DT                                 AS                                 CLM_CLM_STS_EFF_DT 
		, CLM_CLM_STS_END_DT                                 AS                                 CLM_CLM_STS_END_DT 
		, TRIM( CLM_STT_TYP_CD )                             AS                                     CLM_STT_TYP_CD 
		, TRIM( CLM_NO )                                     AS                                             CLM_NO 
		, CLM_AGRE_ID                                        AS                                        CLM_AGRE_ID 
		, TRIM( CLM_REL_SNPSHT_IND )                         AS                                 CLM_REL_SNPSHT_IND 
		, TRIM( CLM_STT_TYP_NM )                             AS                                     CLM_STT_TYP_NM 
		, TRIM( CLM_STS_TYP_CD )                             AS                                     CLM_STS_TYP_CD 
		, TRIM( CLM_STS_TYP_NM )                             AS                                     CLM_STS_TYP_NM 
		, TRIM( CLM_TRANS_RSN_TYP_CD )                       AS                               CLM_TRANS_RSN_TYP_CD 
		, TRIM( CLM_TRANS_RSN_TYP_NM )                       AS                               CLM_TRANS_RSN_TYP_NM 
		, TRIM( CLM_CLM_STS_RSN_COMT )                       AS                               CLM_CLM_STS_RSN_COMT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, HIST_EFF_DTM::DATE                                 AS                                       HIST_EFF_DTM 
		, NVL(HIST_END_DTM ,'12/31/2099')::DATE              AS                                       HIST_END_DTM 
		, TRIM( CSTT_CLM_STT_TYP_CD )                        AS                                CSTT_CLM_STT_TYP_CD 
		, TRIM( CSTT_VOID_IND )                              AS                                      CSTT_VOID_IND 
		, TRIM( CSTS_CLM_STS_TYP_CD )                        AS                                CSTS_CLM_STS_TYP_CD 
		, TRIM( CSTS_VOID_IND )                              AS                                      CSTS_VOID_IND 
		, TRIM( CTRT_CLM_TRANS_RSN_TYP_CD )                  AS                          CTRT_CLM_TRANS_RSN_TYP_CD 
		, TRIM( CTRT_VOID_IND )                              AS                                      CTRT_VOID_IND 
		from SRC_CSH
            )

---- RENAME LAYER ----
,

RENAME_CSH as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID
		, CLM_CLM_STS_ID                                     as                                     CLM_CLM_STS_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_CLM_STS_STT_DT                                 as                                 CLM_CLM_STS_STT_DT
		, CLM_CLM_STS_EFF_DT                                 as                                 CLM_CLM_STS_EFF_DT
		, CLM_CLM_STS_END_DT                                 as                                 CLM_CLM_STS_END_DT
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD
		, CLM_NO                                             as                                             CLM_NO
		, CLM_AGRE_ID                                        as                                        CLM_AGRE_ID
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND
		, CLM_STT_TYP_NM                                     as                                     CLM_STT_TYP_NM
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD
		, CLM_STS_TYP_NM                                     as                                     CLM_STS_TYP_NM
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD
		, CLM_TRANS_RSN_TYP_NM                               as                               CLM_TRANS_RSN_TYP_NM
		, CLM_CLM_STS_RSN_COMT                               as                               CLM_CLM_STS_RSN_COMT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM
		, HIST_END_DTM                                       as                                       HIST_END_DTM
		, CSTT_CLM_STT_TYP_CD                                as                                CSTT_CLM_STT_TYP_CD
		, CSTT_VOID_IND                                      as                                      CSTT_VOID_IND
		, CSTS_CLM_STS_TYP_CD                                as                                CSTS_CLM_STS_TYP_CD
		, CSTS_VOID_IND                                      as                                      CSTS_VOID_IND
		, CTRT_CLM_TRANS_RSN_TYP_CD                          as                          CTRT_CLM_TRANS_RSN_TYP_CD
		, CTRT_VOID_IND                                      as                                      CTRT_VOID_IND 
				FROM     LOGIC_CSH   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CSH                            as ( SELECT * from    RENAME_CSH 
				WHERE CSTT_VOID_IND = 'N' AND CSTS_VOID_IND = 'N' AND CTRT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

 JOIN_CSH  as  ( SELECT * 
				FROM  FILTER_CSH ) 
 
---ETL LAYER
-- STEP1 - Set a event number for the merge if consecutive rows with the same state/status/reasons
 
 , STP1 AS (SELECT 
 *,
 CONDITIONAL_CHANGE_EVENT(CLM_STT_TYP_CD||CLM_STS_TYP_CD||CLM_TRANS_RSN_TYP_CD||NVL(CLM_CLM_STS_RSN_COMT, 'Z')) OVER (PARTITION BY CLM_NO ORDER BY HIST_EFF_DTM, HIST_END_DTM) AS CCE
  FROM  JOIN_CSH ) ,
  
  
-- STEP2 -MERGE IF CONSECUTIVE ROWS WITH THE SAME STATUSES/REASONS  

STP2 AS (SELECT AGRE_ID, CLM_NO, CLM_STT_TYP_CD, CLM_STT_TYP_NM, CLM_STS_TYP_CD, CLM_STS_TYP_NM
, CLM_TRANS_RSN_TYP_CD, CLM_TRANS_RSN_TYP_NM, CLM_CLM_STS_RSN_COMT 
, MIN(CLM_CLM_STS_STT_DT) CLM_CLM_STS_STT_DT
, MIN(CLM_CLM_STS_EFF_DT) CLM_CLM_STS_EFF_DT, NULLIF(MAX(NVL(CLM_CLM_STS_END_DT,'9999-12-31')), '9999-12-31') AS CLM_CLM_STS_END_DT
, MAX(HIST_ID)HIST_ID, MIN(HIST_EFF_DTM) HIST_EFF_DTM, MAX(HIST_END_DTM)HIST_END_DTM
FROM STP1
GROUP BY 1,2,3,4,5,6,7,8,9, CCE ),


-- STEP3 RESOLVE INTRA DAY CHANGES WITH DIFFERENT STATUS 

STP3 AS (
SELECT * FROM STP2
QUALIFY (ROW_NUMBER()OVER (PARTITION BY CLM_NO,HIST_EFF_DTM::DATE ORDER BY HIST_EFF_DTM DESC, HIST_END_DTM DESC)) =1)


-- STEP4 UPDATE THE END DATE WHEN IT HAS CONFLICTS/OVERLAPS. I.E. HIST END DATE-1
SELECT 
HIST_ID
, AGRE_ID
, CLM_NO
, CLM_STT_TYP_CD
, CLM_STT_TYP_NM
, CLM_STS_TYP_CD
, CLM_STS_TYP_NM
, CLM_TRANS_RSN_TYP_CD
, CLM_TRANS_RSN_TYP_NM
, CLM_CLM_STS_RSN_COMT
, CLM_CLM_STS_STT_DT
, CLM_CLM_STS_EFF_DT
, CLM_CLM_STS_END_DT
, HIST_EFF_DTM AS HIST_EFF_DT
, NULLIF(CASE WHEN CLM_NO = LEAD(CLM_NO) OVER (ORDER BY CLM_NO , HIST_EFF_DTM,HIST_END_DTM )
    AND HIST_END_DTM = LEAD(HIST_EFF_DTM) OVER (PARTITION BY CLM_NO ORDER BY HIST_EFF_DTM,HIST_END_DTM )
    THEN dateadd(day, -1, HIST_END_DTM)  ELSE HIST_END_DTM END, '2099-12-31') AS HIST_END_DT
FROM STP3
      );
    